import asyncio
import aiohttp
from db import Connection
from network import Async
from dotenv import load_dotenv
from .task import Task
import time
import sys
import datetime
import os
from log import logger

load_dotenv()
webhook_genwarlog = os.environ["GENWARLOG"]
webhook_anowarlog = os.environ["ANOWARLOG"]

class TerritoryTrackTask(Task):
    def __init__(self, start_after, sleep, wsconns, cede_task):
        super().__init__(start_after, sleep)
        self.wsconns = wsconns
        self.cede_task = cede_task
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def terr_tracker_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("TERRITORY TRACK START")
                start = time.time()

                URL = "https://api.wynncraft.com/v3/guild/list/territory"
                terrs = await Async.get(URL)
                if not isinstance(terrs, dict) or not terrs:
                    logger.warning(
                        "Territory fetch returned no usable snapshot (type=%s); retrying in %ss",
                        type(terrs).__name__, self.sleep
                    )
                    await asyncio.sleep(self.sleep)
                    continue

                old_terrs = {x[0]: x[1] for x in Connection.execute("SELECT * FROM territories")}

                # guild_terr_cnt = {terrs[terr]["guild"]["name"]: 0 for terr in terrs}

                queries = []
                ws_payload = []
                insert_exchanges = []

                claim_res = Connection.execute("SELECT * FROM ally_claims")
                claim_owner = {claim: guild for guild, claim in claim_res}
                allied_guilds = {*claim_owner.values()}
                    
                ally_stats_res = Connection.execute("SELECT * FROM ally_stats")
                ally_stats = {}
                for old_ally_record in ally_stats_res:
                    ally_stats[old_ally_record[0]] = list(old_ally_record[1:])

                if "N/A" in allied_guilds: allied_guilds.remove("N/A")

                guild_specific_log_xchg = []
                for ter in terrs:
                    # guild_terr_cnt[terrs[ter]["guild"]["name"]] += 1
                    if not ter in old_terrs:
                        # new territory. should rarely happen
                        queries.append(f"INSERT INTO territories VALUES (\"{ter}\", \"{terrs[ter]['guild']['name']}\", \"none\");")

                    elif terrs[ter]["guild"]["name"] != old_terrs[ter]:
                        defender, attacker = old_terrs[ter], terrs[ter]['guild']["name"]
                        ws_payload.append('{"defender": "%s", "territory": "%s", "attacker": "%s"}' % 
                                            (defender, ter, attacker))
                        # Alliance stuff
                        if attacker in allied_guilds:
                            if not attacker in ally_stats:
                                # { "FFA":0, "Reclaim":0, "adj help":0, "Other":0, "nom help": 0 }
                                ally_stats[attacker] = [0]*5

                            terr_owner = claim_owner.get(ter, "N/A") # null case if new ter isn't registered yet
                            ally_stats[attacker][0] += terr_owner == "N/A"
                            ally_stats[attacker][1] += terr_owner == attacker

                            # adjusted helps (before adjusting)
                            is_help = attacker != terr_owner and terr_owner != "N/A" and terr_owner in allied_guilds and not defender in allied_guilds
                            ally_stats[attacker][2] += is_help
                            # nom help
                            ally_stats[attacker][4] += is_help
                            # for cede tracking
                            if not attacker in self.cede_task.valor_delta:
                                self.cede_task.valor_delta[attacker] = 0
                            self.cede_task.valor_delta[attacker] += 1

                            ally_stats[attacker][3] += defender == terr_owner and terr_owner in allied_guilds and attacker in allied_guilds # ally-ally cede

                        acquired = terrs[ter]["acquired"][:-1]
                        acquired = datetime.datetime.fromisoformat(acquired)
                        
                        insert_exchanges.append(f"({int(acquired.timestamp())}, \"{defender}\", \"{attacker}\", \"{ter}\")")
                        queries.append(f"UPDATE territories SET guild=\"{attacker}\" WHERE name=\"{ter}\";")

                        if defender == "Titans Valor" or attacker == "Titans Valor":
                            guild_specific_log_xchg.append('{"defender": "%s", "territory": "%s", "attacker": "%s"}' % 
                                            (defender, ter, attacker))
                
                replace_ally_stats = [f"(\"{guild}\", {ally_stats[guild][0]}, {ally_stats[guild][1]}, {ally_stats[guild][2]}, {ally_stats[guild][3]}, {ally_stats[guild][4]})"
                    for guild in ally_stats]

                if len(queries):
                    Connection.exec_all(queries)
                    Connection.execute("INSERT INTO terr_exchange VALUES "+','.join(insert_exchanges))
                    Connection.execute("REPLACE INTO ally_stats VALUES "+','.join(replace_ally_stats))

                    if len(guild_specific_log_xchg):
                        await Async.post(webhook_anowarlog, {"content": '\n'.join(ws_payload)})
                    await Async.post(webhook_genwarlog, {"content": '\n'.join(ws_payload)})

                # Connection.execute("INSERT INTO terr_count VALUES "+
                #     ','.join(f"({int(time.time())}, \"{k}\", {guild_terr_cnt[k]})" for k in guild_terr_cnt))

                for ws in self.wsconns:
                    await ws.send('{"type":"terr","data":'+f"[{','.join(ws_payload)}]" + "}")
                # post to websocket

                end = time.time()
                logger.info("TERRITORY TRACKER"+f" {end-start}s")

                await asyncio.sleep(self.sleep)
        
            logger.info("TerritoryTrackTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(terr_tracker_task))
        