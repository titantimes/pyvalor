import asyncio
from db import Connection
from network import Async
from .task import Task
import time
from log import logger

class PlayerCountTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)

    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        first_run = True
        async def player_count_task():
            nonlocal first_run
            if first_run:
                first_run = False
                await asyncio.sleep(self.start_after)

            start = time.time()
            res = await Async.get("https://api.wynncraft.com/v3/player")
            total = res.get("total")
            if total is None:
                total = len(res.get("players", []))

            Connection.execute("INSERT INTO player_count (time, count) VALUES (%s, %s)",
                               prep_values=[int(start), int(total)])
            await asyncio.sleep(max(0, self.sleep - 10 - (time.time() - start)))

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(player_count_task))
