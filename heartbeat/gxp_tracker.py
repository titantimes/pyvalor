import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
import datetime
import time
import sys
import math
from log import logger
import traceback

gxpLevelExceptions = ["Titans Valor", "The Aquarium", "Avicia", "Empire of Sindria", "KongoBoys", "Paladins United", "Nerfuria", "Eden", "Idiot Co", "Hesperides", "The Broken Gasmask", "Anime Lovers", "TruthSword", "Emipre of TKW", "Black Fangs", "Profession Heaven", "Chiefs Of Corkus", "Cirrus", "HackForums", "Emorians", "The Simple Ones", "Sins of Seedia", "IceBlue Team", "Polish Hussars"]

class GXPTrackerTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)

    @staticmethod
    def level_to_xp(level):
        if level >= 130:
            return 885689 * math.exp(0.139808 * 130)
        return 885689 * math.exp(0.139808 * level)

    @staticmethod
    def xp_to_float_level(xp):
        return math.log(xp / 885689) / 0.139808

    @staticmethod
    def level_pct_to_float(level, pct):
        xpToCurr = GXPTrackerTask.level_to_xp(level)
        xpToNext = GXPTrackerTask.level_to_xp(level + 1)
        currXp = xpToCurr + pct * (xpToNext - xpToCurr)
        return GXPTrackerTask.xp_to_float_level(currXp)

    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False

        async def gxp_tracker_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("GXP START")
                start = time.time()
                end = start

                guildRows = Connection.execute("SELECT guild FROM guild_tracking_schedule ORDER BY tier DESC, dailyGraids DESC;")
                guildList = [g[0] for g in guildRows] if guildRows else []

                for guild in gxpLevelExceptions:
                    if guild not in guildList:
                        guildList.insert(0, guild)

                res = Connection.execute("SELECT uuid, value FROM player_global_stats WHERE label='gu_gxp'")
                prevMemberGxps = {}
                for uuid, value in res:
                    prevMemberGxps[uuid] = value

                for guild in guildList:
                    guildUrl = f"https://api.wynncraft.com/v3/guild/{guild}"
                    guildData = await Async.get(guildUrl)
                    if guildData is None or "members" not in guildData or "level" not in guildData:
                        continue

                    guildLevel = guildData["level"]
                    guildPercent = guildData["xpPercent"] * 0.01
                    _ = GXPTrackerTask.level_pct_to_float(guildLevel, guildPercent)

                    if guildLevel >= 130:
                        guReqToNextXp = 10367116453807
                    else:
                        guReqToNextXp = GXPTrackerTask.level_to_xp(guildLevel + 1) - GXPTrackerTask.level_to_xp(guildLevel)

                    countRaidThreshold = 1 / 1.15 * guReqToNextXp / 1000 / 4

                    members = []
                    insertGxpDeltas = []
                    updateGxpValues = []
                    insertRaidDeltas = []

                    for rank in guildData["members"]:
                        if type(guildData["members"][rank]) != dict:
                            continue
                        for memberName in guildData["members"][rank]:
                            memberFields = guildData["members"][rank][memberName]
                            members.append({"name": memberName, **memberFields})
                            gxpDelta = memberFields["contributed"] - prevMemberGxps.get(memberFields["uuid"], memberFields["contributed"])
                            updateGxpValues.append((memberFields["uuid"], memberFields["contributed"]))
                            if gxpDelta > 0:
                                insertGxpDeltas.append((memberFields["uuid"], gxpDelta))

                    for memberUuid, gxpDelta in insertGxpDeltas:
                        if guildLevel >= 95 and gxpDelta >= countRaidThreshold and countRaidThreshold > 0:
                            numRaids = gxpDelta // countRaidThreshold
                            insertRaidDeltas.append((memberUuid, guild, start, numRaids))

                    if guild == "Titans Valor":
                        queryResult = Connection.execute("SELECT * FROM user_total_xps")
                        uuidToXp = {x[4]: x[:4] for x in queryResult}

                        newQueries = []
                        newMembers = []

                        for member in members:
                            if member["uuid"] not in uuidToXp:
                                newMembers.append(
                                    f"(\"{member['name']}\",{member['contributed']},{member['contributed']},\"Titans Valor\",\"{member['uuid']}\")"
                                )
                            elif member["contributed"] < uuidToXp[member["uuid"]][2]:
                                newXp = uuidToXp[member["uuid"]][1] + member["contributed"]
                                newQueries.append(
                                    f"UPDATE user_total_xps SET xp={newXp}, last_xp={member['contributed']} WHERE uuid=\"{member['uuid']}\";"
                                )
                            elif member["contributed"] > uuidToXp[member["uuid"]][2]:
                                delta = member["contributed"] - uuidToXp[member["uuid"]][2]
                                newXp = uuidToXp[member["uuid"]][1] + delta
                                newQueries.append(
                                    f"UPDATE user_total_xps SET xp={newXp}, last_xp={member['contributed']} WHERE uuid=\"{member['uuid']}\";"
                                )

                        if newMembers:
                            Connection.execute(f"INSERT INTO user_total_xps VALUES {','.join(newMembers)};")
                        if newQueries:
                            Connection.exec_all(newQueries)

                    if insertRaidDeltas:
                        query = "INSERT INTO guild_raid_records VALUES " + ("(%s, %s, %s, %s)," * len(insertRaidDeltas))[:-1]
                        try:
                            Connection.execute(query, prep_values=[value for row in insertRaidDeltas for value in row], fetchall=False)
                        except Exception as raidInsertErr:
                            logger.error(
                                "Batch insert into guild_raid_records failed (%s). Falling back to per-row insert.",
                                raidInsertErr,
                            )
                            logger.error(traceback.format_exc())
                            singleRowQuery = "INSERT INTO guild_raid_records VALUES (%s, %s, %s, %s)"
                            for row in insertRaidDeltas:
                                try:
                                    Connection.execute(singleRowQuery, prepared=True, prep_values=list(row), fetchall=False)
                                except Exception as rowInsertErr:
                                    logger.error(
                                        "Skipping raid record row after insert failure: row=%s err=%s",
                                        row,
                                        rowInsertErr,
                                    )

                    if insertGxpDeltas:
                        query = "INSERT INTO player_delta_record VALUES " + \
                            ",".join(f"('{uuid}', '{guild}', {start}, 'gu_gxp', {gxpDelta})" for uuid, gxpDelta in insertGxpDeltas)
                        Connection.execute(query)

                    if updateGxpValues:
                        query = "REPLACE INTO player_global_stats VALUES " + \
                            ",".join(f"('{uuid}', 'gu_gxp', {value})" for uuid, value in updateGxpValues)
                        Connection.execute(query)

                    end = time.time()
                    await asyncio.sleep(0.3)

                logger.info("GXP TRACKER" + f" {end-start}s")
                await asyncio.sleep(self.sleep)

            logger.info("GXPTrackerTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(gxp_tracker_task))
