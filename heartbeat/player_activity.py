import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
from collections import defaultdict
import time
import datetime
from log import logger
from urllib.parse import quote

class PlayerActivityTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def player_activity_task():
            await asyncio.sleep(self.start_after)

            logger.info("PLAYER ACTIVITY TRACK START")
            start = time.time()
            
            onlineResponse = await Async.get("https://api.wynncraft.com/v3/player")
            if not isinstance(onlineResponse, dict) or "players" not in onlineResponse:
                logger.warning("PLAYER ACTIVITY TASK: invalid /v3/player response")
                await asyncio.sleep(self.sleep)
                return

            playersData = onlineResponse["players"]
            if isinstance(playersData, list):
                online_all = set(playersData)
            elif isinstance(playersData, dict):
                online_all = set()
                for worldPlayers in playersData.values():
                    if isinstance(worldPlayers, list):
                        for playerName in worldPlayers:
                            online_all.add(playerName)
            else:
                logger.warning("PLAYER ACTIVITY TASK: some fields are missing or some")
                await asyncio.sleep(self.sleep)
                return

            scheduledGuilds = Connection.execute("SELECT guild FROM guild_tracking_schedule WHERE tier > 0")
            guildsList = [g[0] for g in scheduledGuilds] if scheduledGuilds else []

            player_to_guild = {}
            syncedGuilds = []
            inserts = []

            for guild in guildsList:
                guild_data = await Async.get("https://api.wynncraft.com/v3/guild/" + quote(guild, safe=''))
                if not isinstance(guild_data, dict):
                    continue
                guild_members = []
                if guild_data is None or not "members" in guild_data: continue
                    
                for rank in guild_data["members"]:
                    if isinstance(guild_data["members"][rank], int): continue
                    guild_members.extend((x, guild_data["members"][rank][x]["uuid"]) for x in guild_data["members"][rank])
                
                for member, uuid in guild_members:
                    player_to_guild[member] = guild, uuid
                syncedGuilds.append(guild)

            syncedGuilds = list(set(syncedGuilds))

            intersection = online_all & player_to_guild.keys()

            for player_name in intersection:
                guild, uuid = player_to_guild[player_name]

                if not player_name or not guild or not uuid: 
                    continue

                inserts.append(f"(\"{player_name}\", \"{guild}\", {int(time.time())}, \"{uuid}\")")

            logger.info(
                f"PLAYER ACTIVITY MATCHES online={len(online_all)} trackedMembers={len(player_to_guild)} inserts={len(inserts)}"
            )

            for i in range(0, len(inserts), 32):
                try:
                    Connection.execute(f"INSERT INTO activity_members VALUES {','.join(inserts[i:i+32])}")
                except Exception as e:
                    logger.info(f"PLAYER ACTIVITY TASK ERROR")
                    logger.exception(e)
                    logger.warn(f"insertion looks like: {','.join(inserts[i:i+32])}")
                    self.finished = True

            end = time.time()
            logger.info("PLAYER ACTIVITY TASK"+f" {end-start}s")
            
            await asyncio.sleep(self.sleep)

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(player_activity_task))
        
