import asyncio
import aiohttp
from db import Connection
from network import Async
from dotenv import load_dotenv
from .task import Task
import time
import datetime
import os
from log import logger

load_dotenv()
webhook = os.environ["JOINLEAVE"]

class GuildActivityTask(Task):
    def __init__(self, start_after, sleep, wsconns):
        super().__init__(start_after, sleep)
        self.wsconns = wsconns
        self.guildmembers_check = None
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def guild_activity_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("GUILD ACTIVITY TRACK START")
                start = time.time()
                now = int(start)

                tvResponse = await Async.get("https://api.wynncraft.com/v3/guild/Titans%20Valor")
                if not isinstance(tvResponse, dict) or "members" not in tvResponse or not isinstance(tvResponse["members"], dict):
                    logger.warning("GUILD ACTIVITY TASK: invalid Titans Valor guild response")
                    await asyncio.sleep(self.sleep)
                    continue

                guildmembers_data = tvResponse["members"]
                currentguild = set()
                for rank in guildmembers_data:
                    if type(guildmembers_data[rank]) != dict: continue
                    currentguild |= guildmembers_data[rank].keys()

                if self.guildmembers_check is None:
                    try:
                        rows = Connection.execute("SELECT name FROM guild_member_cache WHERE guild='Titans Valor'")
                        self.guildmembers_check = {r[0] for r in rows} if rows else set()
                    except Exception:
                        self.guildmembers_check = set()

                old_members = set(self.guildmembers_check)
                left = [f'"{x}"' for x in old_members - currentguild]
                join = [f'"{x}"' for x in currentguild - old_members]
                
                if left or join:
                    for ws in self.wsconns:
                        await ws.send('{"type":"join","leave":'+f'[{",".join(left)}],"join":'+f'[{",".join(join)}]' + "}")
                    await Async.post(webhook, {"content": f"Joined: {repr(join)}\nLeft: {repr(left)}"})

                try:
                    Connection.execute("DELETE FROM guild_member_cache WHERE guild='Titans Valor'")
                    if currentguild:
                        Connection.execute("INSERT INTO guild_member_cache VALUES "+",".join(f"('Titans Valor','{x}')" for x in currentguild))
                except Exception:
                    logger.debug("Failed to update guild_member_cache in DB")

                self.guildmembers_check = set(currentguild)
                
                scheduledGuilds = Connection.execute("SELECT guild FROM guild_tracking_schedule WHERE nextSync <= %s", prep_values=[now])
                guildsList = [g[0] for g in scheduledGuilds] if scheduledGuilds else []
                guildMemberCnt = {}
                
                for guild in guildsList:
                    try:
                        guildUrl = f"https://api.wynncraft.com/v3/guild/{guild.replace(' ', '%20')}"
                        guildResponse = await Async.get(guildUrl)
                        if "online" in guildResponse:
                            guildMemberCnt[guild] = guildResponse["online"]
                        else:
                            guildMemberCnt[guild] = 0
                    except Exception as e:
                        logger.error(f"Failed to fetch online count for guild {guild}: {e}")
                        guildMemberCnt[guild] = 0

                if guildMemberCnt:
                    insertValues = ','.join(f"(\"{guild}\", {guildMemberCnt[guild]}, {now})" for guild in guildMemberCnt)
                    if insertValues:
                        Connection.execute("INSERT INTO guild_member_count VALUES " + insertValues)
                        logger.info(f"Inserted guild member counts for {len(guildMemberCnt)} guilds")
                    
                    for guild in guildsList:
                        Connection.execute(
                            """
UPDATE guild_tracking_schedule
SET lastSync = %s,
    nextSync = %s + CASE tier
        WHEN 3 THEN 300
        WHEN 2 THEN 600
        WHEN 1 THEN 1800
        ELSE 1800
    END
WHERE guild = %s
""",
                            prep_values=[now, now, guild]
                        )
                else:
                    logger.info("No guild member data to insert")

                end = time.time()
                logger.info("GUILD ACTIVITY TASK"+f" {end-start}s")
                
                await asyncio.sleep(self.sleep)
        
            logger.info("GuildActivityTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(guild_activity_task))
