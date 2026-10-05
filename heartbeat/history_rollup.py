import calendar
import datetime

from db import Connection


SECONDS_PER_DAY = 86400
SECONDS_PER_WEEK = 7 * SECONDS_PER_DAY

#arbitrary 1 day aggr start
def threemonf(now: float) -> int:
	current = datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
	month_index = current.year * 12 + current.month - 1 - 3
	year, month_zero_based = divmod(month_index, 12)
	month = month_zero_based + 1
	day = min(current.day, calendar.monthrange(year, month)[1])
	return int(current.replace(year=year, month=month, day=day).timestamp())

#arbitrary week start
def oneyear(now: float) -> int:
	current = datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
	try:
		previous_year = current.replace(year=current.year - 1)
	except ValueError:
		previous_year = current.replace(year=current.year - 1, day=28)
	return int(previous_year.timestamp())


def lastday(table: str, time_column: str, cutoff: int):
	result = Connection.execute(
		f"SELECT MIN(`{time_column}`) FROM `{table}` "
		f"WHERE is_rollup = 0 AND `{time_column}` < %s",
		prep_values=[cutoff],
	)
	if not result or result[0][0] is None:
		return None
	day_start = int(result[0][0]) // SECONDS_PER_DAY * SECONDS_PER_DAY
	if day_start + SECONDS_PER_DAY > cutoff:
		return None
	return day_start


def startweek(timestamp: int) -> int:
	value = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
	monday = value - datetime.timedelta(days=value.weekday())
	monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
	return int(monday.timestamp())


def lastweek(table: str, time_column: str, cutoff: int):
	result = Connection.execute(
		f"SELECT MIN(`{time_column}`) FROM `{table}` "
		f"WHERE is_week_rollup = 0 AND `{time_column}` < %s",
		prep_values=[cutoff],
	)
	if not result or result[0][0] is None:
		return None
	week_start = startweek(int(result[0][0]))
	if week_start + SECONDS_PER_WEEK > cutoff:
		return None
	return week_start


def daysplit(day_start: int, season_boundaries: set[int]):
	day_end = day_start + SECONDS_PER_DAY
	cuts = [day_start]
	cuts.extend(sorted(boundary for boundary in season_boundaries if day_start < boundary < day_end))
	cuts.append(day_end)
	return list(zip(cuts, cuts[1:]))


def weeksplit(week_start: int, season_boundaries: set[int]):
	week_end = week_start + SECONDS_PER_WEEK
	cuts = [week_start]
	cuts.extend(sorted(boundary for boundary in season_boundaries if week_start < boundary < week_end))
	cuts.append(week_end)
	return list(zip(cuts, cuts[1:]))


def appendi(statements, table: str, columns: str, row_width: int, rows):
	for offset in range(0, len(rows), 500):
		batch = rows[offset:offset + 500]
		values_clause = ",".join(["(" + ",".join(["%s"] * row_width) + ")"] * len(batch))
		values = [value for row in batch for value in row]
		statements.append((f"INSERT INTO `{table}` ({columns}) VALUES {values_clause}", values))


def dayroll(cutoff: int, segments):
	statements = []
	group_count = 0
	for segment_start, segment_end in segments:
		rows = Connection.execute(
			"""
SELECT uuid, guild, MAX(name), MIN(`timestamp`)
FROM activity_members
WHERE is_rollup = 0 AND `timestamp` >= %s AND `timestamp` < %s AND `timestamp` < %s
GROUP BY uuid, guild
""",
			prep_values=[segment_start, segment_end, cutoff],
		)
		if not rows:
			continue

		group_count += len(rows)
		statements.append((
			"DELETE FROM activity_members WHERE is_rollup = 0 AND `timestamp` >= %s AND `timestamp` < %s AND `timestamp` < %s",
			[segment_start, segment_end, cutoff],
		))
		rollups = [(name, guild, first_timestamp, uuid, 1) for uuid, guild, name, first_timestamp in rows]
		appendi(
			statements,
			"activity_members",
			"name, guild, `timestamp`, uuid, is_rollup, is_week_rollup",
			6,
			[(*row, 0) for row in rollups],
		)

	if statements:
		Connection.execute_transaction(statements)
	return group_count


def deltadayRoll(cutoff: int, segments):
	statements = []
	group_count = 0
	for segment_start, segment_end in segments:
		rows = Connection.execute(
			"""
SELECT uuid, guild, label, SUM(delta), MIN(`time`)
FROM player_delta_record
WHERE is_rollup = 0 AND `time` >= %s AND `time` < %s AND `time` < %s
GROUP BY uuid, guild, label
""",
			prep_values=[segment_start, segment_end, cutoff],
		)
		if not rows:
			continue

		group_count += len(rows)
		statements.append((
			"DELETE FROM player_delta_record WHERE is_rollup = 0 AND `time` >= %s AND `time` < %s AND `time` < %s",
			[segment_start, segment_end, cutoff],
		))
		rollups = [(uuid, guild, first_timestamp, label, delta, 1) for uuid, guild, label, delta, first_timestamp in rows]
		appendi(
			statements,
			"player_delta_record",
			"uuid, guild, `time`, label, delta, is_rollup, is_week_rollup",
			7,
			[(*row, 0) for row in rollups],
		)

	if statements:
		Connection.execute_transaction(statements)
	return group_count


def weekroll(cutoff: int, segments):
	statements = []
	group_count = 0
	for segment_start, segment_end in segments:
		rows = Connection.execute(
			"""
SELECT uuid, guild, MAX(name), MIN(`timestamp`)
FROM activity_members
WHERE is_week_rollup = 0 AND `timestamp` >= %s AND `timestamp` < %s AND `timestamp` < %s
GROUP BY uuid, guild
""",
			prep_values=[segment_start, segment_end, cutoff],
		)
		if not rows:
			continue

		group_count += len(rows)
		statements.append((
			"DELETE FROM activity_members WHERE is_week_rollup = 0 AND `timestamp` >= %s AND `timestamp` < %s AND `timestamp` < %s",
			[segment_start, segment_end, cutoff],
		))
		rollups = [(name, guild, first_timestamp, uuid, 1, 1) for uuid, guild, name, first_timestamp in rows]
		appendi(
			statements,
			"activity_members",
			"name, guild, `timestamp`, uuid, is_rollup, is_week_rollup",
			6,
			rollups,
		)

	if statements:
		Connection.execute_transaction(statements)
	return group_count


def deltaweekRoll(cutoff: int, segments):
	statements = []
	group_count = 0
	for segment_start, segment_end in segments:
		rows = Connection.execute(
			"""
SELECT uuid, guild, label, SUM(delta), MIN(`time`)
FROM player_delta_record
WHERE is_week_rollup = 0 AND `time` >= %s AND `time` < %s AND `time` < %s
GROUP BY uuid, guild, label
""",
			prep_values=[segment_start, segment_end, cutoff],
		)
		if not rows:
			continue

		group_count += len(rows)
		statements.append((
			"DELETE FROM player_delta_record WHERE is_week_rollup = 0 AND `time` >= %s AND `time` < %s AND `time` < %s",
			[segment_start, segment_end, cutoff],
		))
		rollups = [(uuid, guild, first_timestamp, label, delta, 1, 1) for uuid, guild, label, delta, first_timestamp in rows]
		appendi(
			statements,
			"player_delta_record",
			"uuid, guild, `time`, label, delta, is_rollup, is_week_rollup",
			7,
			rollups,
		)

	if statements:
		Connection.execute_transaction(statements)
	return group_count


def rolloned(now: float):
	dcut = threemonf(now)
	wcut = oneyear(now)
	seasonal = Connection.execute(
		"SELECT start_time, end_time FROM season_list WHERE LOWER(season_name) <> 'all'"
	)
	season_boundaries = {
		int(boundary)
		for row in (seasonal or [])
		for boundary in row
		if boundary is not None
	}

	rolled = {}
	adayd = lastday("activity_members", "timestamp", dcut)
	if adayd is not None:
		rolled["activity_groups"] = dayroll(
			dcut, daysplit(adayd, season_boundaries)
		)

	dday = lastday("player_delta_record", "time", dcut)
	if dday is not None:
		rolled["delta_groups"] = deltadayRoll(
			dcut, daysplit(dday, season_boundaries)
		)

	aweek = lastweek("activity_members", "timestamp", wcut)
	if aweek is not None:
		rolled["activity_week_groups"] = weekroll(
			wcut, weeksplit(aweek, season_boundaries)
		)

	aweek = lastweek("player_delta_record", "time", wcut)
	if aweek is not None:
		rolled["delta_week_groups"] = deltaweekRoll(
			wcut, weeksplit(aweek, season_boundaries)
		)

	return rolled
