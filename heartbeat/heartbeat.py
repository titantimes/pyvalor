from .task import Task
from .terr_tracker import TerritoryTrackTask
from .player_activity import PlayerActivityTask
from .gxp_tracker import GXPTrackerTask
from .guild_activity import GuildActivityTask
from .player_stats import PlayerStatsTask
from .guild_tag import GuildTagTask
from .cede_tracker import CedeTrackTask
from .wc_players import WCPlayersTask
from .guild_schedule_tracker import GuildScheduleTrackerTask
from .season_rating_tracker import SeasonRatingTrackerTask
from .player_last_join import PlayerLastJoinTask
from .reclaim_tracker import ReclaimTrackerTask
from dotenv import load_dotenv
from log import logger
import asyncio
import os

load_dotenv()
enabled = [x.strip().lower() for x in os.environ["ENABLED"].split(',') if x.strip()]

class Heartbeat:
    wsconns = set()
    cede_tracker = CedeTrackTask(0, 3600*2)

    tasks = [
        TerritoryTrackTask(2, 60, wsconns, cede_tracker),
        PlayerActivityTask(3, 3600),
        GXPTrackerTask(5, 60),
        GuildActivityTask(61, 300, wsconns),
        PlayerStatsTask(101, 3600),
        GuildTagTask(41, 3600),
        GuildScheduleTrackerTask(29, 300),
        SeasonRatingTrackerTask(223, 21600),
        PlayerLastJoinTask(31, 120),
        ReclaimTrackerTask(17, 8)
    ]
    
    @staticmethod
    def run_tasks():
        logger.info(f"ENABLED TASK TOKENS: {enabled}")
        for t in Heartbeat.tasks:
            taskName = t.__class__.__name__.lower()
            if taskName not in enabled:
                logger.info(f"SKIP TASK {taskName}")
                continue
            logger.info(f"START TASK {taskName}")
            t.run()

    @staticmethod
    def stop_tasks():
        for t in Heartbeat.tasks:
            if not t.__class__.__name__.lower() in enabled: continue
            t.stop()
