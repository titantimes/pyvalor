import asyncio
import time
from db import Connection
from network import Async
from .task import Task
from log import logger


class ReclaimTrackerTask(Task):
    def __init__(self, startafter, sleep):
        super().__init__(startafter, sleep)
        self.anoguildname = "Titans Valor"
        self.anoguildtag = "ANO"
        #dread wrote these lists blame him if a terr is spelt wrong
        self.targetterritories = set([
            "Nodguj Nation",
            "Jofash Tunnel",
            "Maro Peaks",
            "Volcanic Isles",
            "Dreary Docks",
            "Pirate Town",
            "Aldorei Valley Outskirts",
            "Lost Atoll",
            "Regular Island",
            "Dujgon Nation",
            "Half Moon Island",
            "Rooster Island",
            "Derelict Mansion",
            "Disturbed Crypt",
            "Jofash Docks",
            "Icy Island",
            "Skien's Island",
            "Tree Island",
            "Durum Oat Islet",
            "Zhight Island",
            "The Shiar",
            "Durum Isles Barn",
            "Lifeless Forest",
            "Mage Island",
            "Durum Barley Islet",
            "Selchar",
            "Luxuriant Pond",
            "Bear Zoo",
            "Durum Malt Islet",
            "Light Peninsula",
            "Santa's Hideout"
        ])

        self.smallsnakepool = set([
            "Dreary Docks",
            "Pirate Town",
            "Aldorei Valley Outskirts",
            "Regular Island",
            "Rooster Island",
            "Derelict Mansion",
            "Disturbed Crypt",
            "Zhight Island",
            "Lifeless Forest",
            "Luxuriant Pond",
            "Bear Zoo",
            "Durum Malt Islet",
        ])

        self.snipeterritory = "Nodguj Nation"

        self.wipestarterterritory = "Nodguj Nation"
        self.wipeminterritories = 11

    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def isanoowner(self, guilddata):
        if not isinstance(guilddata, dict):
            return False
        guildname = guilddata.get("name", "")
        guildtag = guilddata.get("prefix", "")
        return guildname == self.anoguildname or guildtag == self.anoguildtag

    async def fetchwarcounts(self):
        guildurl = "https://api.wynncraft.com/v3/guild/Titans%20Valor"
        guildres = await Async.get(guildurl)
        if not isinstance(guildres, dict):
            return {}
        membersdata = guildres.get("members", {})
        if not isinstance(membersdata, dict):
            return {}

        warmap = {}
        for rankname, rankmembers in membersdata.items():
            if not isinstance(rankmembers, dict):
                continue
            for membername, memberdata in rankmembers.items():
                if not isinstance(memberdata, dict):
                    continue
                memberuuid = memberdata.get("uuid")
                globaldata = memberdata.get("globalData", {})
                memberwars = globaldata.get("wars", 0) if isinstance(globaldata, dict) else 0
                if memberuuid:
                    warmap[memberuuid] = int(memberwars)
        return warmap

    def classifyraid(self, lostevents, durationseconds, reclaimstarted):
        uniquelost = set(lostevents)

        if reclaimstarted:
            return "reclaim"

        if self.snipeterritory and self.snipeterritory in uniquelost:
            uniquelostcount = len(uniquelost)
            if uniquelostcount >= self.wipeminterritories:
                return "wipe"
            return "snipe"

        if self.wipestarterterritory and self.wipestarterterritory in uniquelost:
            if len(uniquelost) >= self.wipeminterritories:
                return "wipe"

        if len(uniquelost) > 0 and uniquelost.issubset(self.smallsnakepool):
            if durationseconds < 1800:
                return "small"
            return "snake"

        outofpoolterritories = uniquelost - self.smallsnakepool
        if len(outofpoolterritories) >= 1:
            return "big"

        return "unknown"

    def run(self):
        self.finished = False

        async def reclaimtrackertask():
            await asyncio.sleep(self.start_after)

            prevowners = {}
            attackactive = False
            attackstart = 0.0
            recoverystart = None
            attackevents = []
            startwars = {}
            snapshotscheduledat = 0.0
            snapshotready = False
            fallbackdelayseconds = 300
            fallbackevents = []
            firstreclaimat = None
            fullwipehit = False
            reclaimstarted = False

            while not self.finished:
                logger.info("RECLAIM TRACK START")
                loopstart = time.time()

                if len(fallbackevents) > 0:
                    remainingfallbacks = []
                    for fallbackevent in fallbackevents:
                        if time.time() < fallbackevent["checkat"]:
                            remainingfallbacks.append(fallbackevent)
                            continue

                        latewars = await self.fetchwarcounts()
                        if len(latewars) == 0:
                            remainingfallbacks.append(fallbackevent)
                            continue

                        laterows = []
                        for playeruuid, startvalue in fallbackevent["startwars"].items():
                            if playeruuid in fallbackevent["inserteduuids"]:
                                continue

                            endvalue = latewars.get(playeruuid, startvalue)
                            contribution = int(endvalue) - int(startvalue)
                            if contribution < 0:
                                contribution = 0
                            if contribution > fallbackevent["maxcontribution"]:
                                contribution = fallbackevent["maxcontribution"]

                            if contribution > 0:
                                laterows.append((playeruuid, contribution, fallbackevent["attackendstamp"], fallbackevent["raidtype"]))

                        if laterows:
                            query = "INSERT INTO ano_reclaim_records (uuid, contribution, `time`, raid_type) VALUES " + \
                                ",".join(["(%s, %s, %s, %s)"] * len(laterows))
                            flatvalues = []
                            for row in laterows:
                                flatvalues.extend(row)
                            Connection.execute(query, prep_values=flatvalues, fetchall=False)

                    fallbackevents = remainingfallbacks

                terrres = await Async.get("https://api.wynncraft.com/v3/guild/list/territory")
                if not isinstance(terrres, dict):
                    await asyncio.sleep(self.sleep)
                    continue

                if len(self.targetterritories) == 0:
                    await asyncio.sleep(self.sleep)
                    continue

                currentowners = {}
                for territoryname in self.targetterritories:
                    territorydata = terrres.get(territoryname)
                    if not isinstance(territorydata, dict):
                        continue
                    guilddata = territorydata.get("guild", {})
                    guildname = guilddata.get("name", "") if isinstance(guilddata, dict) else ""
                    currentowners[territoryname] = guildname

                if not prevowners:
                    prevowners = dict(currentowners)

                anylost = False
                allowned = True
                ownedcount = 0
                for territoryname in self.targetterritories:
                    territorydata = terrres.get(territoryname)
                    if not isinstance(territorydata, dict):
                        allowned = False
                        continue
                    isowned = self.isanoowner(territorydata.get("guild", {}))
                    if isowned:
                        ownedcount += 1
                    if not isowned:
                        anylost = True
                        allowned = False

                if not attackactive and anylost:
                    attackactive = True
                    attackstart = time.time()
                    recoverystart = None
                    attackevents = []
                    startwars = {}
                    snapshotscheduledat = attackstart + 600
                    snapshotready = False
                    firstreclaimat = None
                    fullwipehit = ownedcount == 0
                    reclaimstarted = False

                    for territoryname in self.targetterritories:
                        territorydata = terrres.get(territoryname)
                        if not isinstance(territorydata, dict):
                            continue
                        if not self.isanoowner(territorydata.get("guild", {})):
                            attackevents.append(territoryname)

                    logger.info("RECLAIM ATTACK START")

                if attackactive:
                    if ownedcount == 0:
                        fullwipehit = True
                    if fullwipehit and ownedcount > 0:
                        reclaimstarted = True

                    if not snapshotready and time.time() >= snapshotscheduledat:
                        startwars = await self.fetchwarcounts()
                        snapshotready = True

                    for territoryname in self.targetterritories:
                        territorydata = terrres.get(territoryname)
                        if not isinstance(territorydata, dict):
                            continue

                        guilddata = territorydata.get("guild", {})
                        ownernow = guilddata.get("name", "") if isinstance(guilddata, dict) else ""
                        ownerprev = prevowners.get(territoryname, "")
                        wasano = ownerprev == self.anoguildname
                        nowano = self.isanoowner(guilddata)

                        if wasano and not nowano:
                            attackevents.append(territoryname)
                        if not wasano and nowano and firstreclaimat is None:
                            firstreclaimat = time.time()

                    if allowned:
                        if recoverystart is None:
                            recoverystart = time.time()
                        elif time.time() - recoverystart >= 1200:
                            endwars = await self.fetchwarcounts()
                            attackendstamp = int(time.time())
                            durationseconds = int(time.time() - attackstart)
                            classtime = durationseconds
                            if firstreclaimat is not None:
                                classtime = int(time.time() - firstreclaimat)
                            raidtype = self.classifyraid(attackevents, classtime, reclaimstarted)
                            maxcontribution = len(attackevents)

                            if not snapshotready:
                                startwars = await self.fetchwarcounts()
                                snapshotready = True

                            insertrows = []
                            for playeruuid, startvalue in startwars.items():
                                endvalue = endwars.get(playeruuid, startvalue)
                                contribution = int(endvalue) - int(startvalue)
                                if contribution < 0:
                                    contribution = 0
                                if contribution > maxcontribution:
                                    contribution = maxcontribution
                                if contribution > 0:
                                    insertrows.append((playeruuid, contribution, attackendstamp, raidtype))

                            if insertrows:
                                query = "INSERT INTO ano_reclaim_records (uuid, contribution, `time`, raid_type) VALUES " + \
                                    ",".join(["(%s, %s, %s, %s)"] * len(insertrows))
                                flatvalues = []
                                for row in insertrows:
                                    flatvalues.extend(row)
                                Connection.execute(query, prep_values=flatvalues, fetchall=False)

                            inserteduuids = {row[0] for row in insertrows}
                            fallbackevents.append({
                                "checkat": time.time() + fallbackdelayseconds,
                                "startwars": dict(startwars),
                                "maxcontribution": maxcontribution,
                                "raidtype": raidtype,
                                "attackendstamp": attackendstamp,
                                "inserteduuids": inserteduuids,
                            })

                            logger.info(
                                f"leave this here for now while i see if it works duration={durationseconds} raidtype={raidtype} territories={len(attackevents)}"
                            )

                            attackactive = False
                            attackstart = 0.0
                            recoverystart = None
                            attackevents = []
                            startwars = {}
                            snapshotscheduledat = 0.0
                            snapshotready = False
                            firstreclaimat = None
                            fullwipehit = False
                            reclaimstarted = False
                    else:
                        recoverystart = None

                prevowners = dict(currentowners)

                loopend = time.time()
                logger.info("RECLAIM TRACK" + f" {loopend-loopstart}s")
                await asyncio.sleep(self.sleep)
                #dont add a finish given this runs like all the time
        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(reclaimtrackertask))
