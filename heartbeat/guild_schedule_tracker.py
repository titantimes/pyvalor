import asyncio
from db import Connection
from .task import Task
import time
from log import logger

graidExceptions = {}
gxpLevelExceptions = ["Titans Valor", "The Aquarium", "Avicia", "Empire of Sindria", "KongoBoys", "Paladins United", "Nerfuria", "Eden", "Idiot Co", "Hesperides", "The Broken Gasmask", "Anime Lovers", "TruthSword", "Emipre of TKW", "Black Fangs", "Profession Heaven", "Chiefs Of Corkus", "Cirrus", "HackForums", "Emorians", "The Simple Ones", "Sins of Seedia", "IceBlue Team", "Polish Hussars"]

tierIntervals = {
    1: 1800,
    2: 600,
    3: 300,
}

class GuildScheduleTrackerTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)

    @staticmethod
    def getTierFromGraids(dailyGraids):
        if dailyGraids >= 200:
            return 3
        elif dailyGraids >= 100:
            return 2
        elif dailyGraids >= 50:
            return 1
        return 0

    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def guildScheduleTracker():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("GUILD SCHEDULE TRACKER START")
                start = time.time()

                try:
                    oneDayAgo = start - 86400
                    twoDaysAgo = start - 86400 * 2
                    graidQuery = """
SELECT guild, SUM(graidcount_diff) as dailyGraids
FROM delta_graids
WHERE time >= %s
GROUP BY guild
"""
                    graidResults = Connection.execute(graidQuery, prep_values=[oneDayAgo])

                    twoDayQuery = """
SELECT guild, SUM(graidcount_diff) as twoDayGraids
FROM delta_graids
WHERE time >= %s
GROUP BY guild
"""
                    twoDayResults = Connection.execute(twoDayQuery, prep_values=[twoDaysAgo])

                    existingRows = Connection.execute("SELECT guild FROM guild_tracking_schedule")
                    existingGuilds = [g[0] for g in existingRows] if existingRows else []
                    
                    guildTierMap = {}
                    for guild, dailyGraids in graidResults:
                        tier = self.getTierFromGraids(dailyGraids) if guild else 0
                        guildTierMap[guild] = (tier, dailyGraids)

                    twoDayMap = {}
                    for guild, twoDayGraids in twoDayResults:
                        twoDayMap[guild] = twoDayGraids

                    guildList = list(set(guildTierMap.keys()) | set(twoDayMap.keys()) | set(gxpLevelExceptions) | set(graidExceptions.keys()) | set(existingGuilds))

                    now = int(start)
                    upserts = []

                    for guild in guildList:
                        if guild in gxpLevelExceptions:
                            tier = 3
                            dailyGraids = guildTierMap[guild][1] if guild in guildTierMap else 0
                        elif guild in graidExceptions:
                            tier = graidExceptions[guild]
                            dailyGraids = 0
                        elif guild in guildTierMap:
                            tier, dailyGraids = guildTierMap[guild]
                        elif twoDayMap.get(guild, 0) >= 100:
                            tier = 1
                            dailyGraids = 0
                        else:
                            tier = 0
                            dailyGraids = 0

                        if tier > 0:
                            interval = tierIntervals.get(tier, 1800)
                            nextSync = now + interval
                            upserts.append((guild, tier, nextSync, now, dailyGraids))

                    if upserts:
                        upsertQuery = """
INSERT INTO guild_tracking_schedule (guild, tier, nextSync, lastSync, dailyGraids)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    tier=VALUES(tier),
    dailyGraids=VALUES(dailyGraids),
    nextSync=IF(tier<>VALUES(tier), VALUES(nextSync), nextSync)
"""
                        for upsert in upserts:
                            Connection.execute(upsertQuery, prep_values=list(upsert))

                        trackedGuilds = [guild for guild, _, _, _, _ in upserts]
                        if trackedGuilds:
                            placeholders = ",".join(["%s"] * len(trackedGuilds))
                            Connection.execute(f"DELETE FROM guild_tracking_schedule WHERE guild NOT IN ({placeholders})", prep_values=trackedGuilds)
                        else:
                            Connection.execute("DELETE FROM guild_tracking_schedule")

                        Connection.execute("DELETE FROM guild_list")
                        if trackedGuilds:
                            listValues = []
                            for guild in trackedGuilds:
                                listValues.append(guild)
                            Connection.execute(
                                "INSERT INTO guild_list (guild) VALUES " + ("(%s)," * len(trackedGuilds))[:-1],
                                prepared=True,
                                prep_values=listValues
                            )
                    else:
                        Connection.execute("DELETE FROM guild_tracking_schedule")
                        Connection.execute("DELETE FROM guild_list")

                except Exception as e:
                    logger.error("GUILD SCHEDULE TRACKER ERROR")
                    logger.exception(e)
                    self.finished = True

                end = time.time()
                logger.info("GUILD SCHEDULE TRACKER" + f" {end-start}s")

                await asyncio.sleep(self.sleep)

            logger.info("GuildScheduleTrackerTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(guildScheduleTracker))
