import calendar
import datetime

from db import Connection

SECONDS_PER_DAY = 86400
def _three_month_cutoff(now: float) -> int:
	current = datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
	month_index = current.year * 12 + current.month - 1 - 3
	year, month_zero_based = divmod(month_index, 12)
	month = month_zero_based + 1
	day = min(current.day, calendar.monthrange(year, month)[1])
	return int(current.replace(year=year, month=month, day=day).timestamp())


def _oldest_complete_day(table: str, time_column: str, cutoff: int):
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


def _day_segments(day_start: int, season_boundaries: set[int]):
	day_end = day_start + SECONDS_PER_DAY
	cuts = [day_start]
	cuts.extend(sorted(boundary for boundary in season_boundaries if day_start < boundary < day_end))
	cuts.append(day_end)
	return list(zip(cuts, cuts[1:]))


def _append_inserts(statements, table: str, columns: str, row_width: int, rows):
	for offset in range(0, len(rows), 500):
		batch = rows[offset:offset + 500]
		values_clause = ",".join(["(" + ",".join(["%s"] * row_width) + ")"] * len(batch))
		values = [value for row in batch for value in row]
		statements.append((f"INSERT INTO `{table}` ({columns}) VALUES {values_clause}", values))


def _rollup_activity_day(cutoff: int, segments):
	statements = []
	group_count = 0
	for segment_start, segment_end in segments:
		rows = Connection.execute(
			"""
SELECT uuid, guild, MAX(name)
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
		rollups = [(name, guild, segment_start, uuid, 1) for uuid, guild, name in rows]
		_append_inserts(
			statements,
			"activity_members",
			"name, guild, `timestamp`, uuid, is_rollup",
			5,
			rollups,
		)

	if statements:
		Connection.execute_transaction(statements)
	return group_count


def _rollup_player_deltas_day(cutoff: int, segments):
	statements = []
	group_count = 0
	for segment_start, segment_end in segments:
		rows = Connection.execute(
			"""
SELECT uuid, guild, label, SUM(delta)
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
		rollups = [(uuid, guild, segment_start, label, delta, 1) for uuid, guild, label, delta in rows]
		_append_inserts(
			statements,
			"player_delta_record",
			"uuid, guild, `time`, label, delta, is_rollup",
			6,
			rollups,
		)

	if statements:
		Connection.execute_transaction(statements)
	return group_count


def rollup_one_eligible_day(now: float):
	cutoff = _three_month_cutoff(now)
	season_rows = Connection.execute(
		"SELECT start_time, end_time FROM season_list WHERE LOWER(season_name) <> 'all'"
	)
	season_boundaries = {
		int(boundary)
		for row in (season_rows or [])
		for boundary in row
		if boundary is not None
	}

	rolled = {}
	activity_day = _oldest_complete_day("activity_members", "timestamp", cutoff)
	if activity_day is not None:
		rolled["activity_groups"] = _rollup_activity_day(
			cutoff, _day_segments(activity_day, season_boundaries)
		)

	delta_day = _oldest_complete_day("player_delta_record", "time", cutoff)
	if delta_day is not None:
		rolled["delta_groups"] = _rollup_player_deltas_day(
			cutoff, _day_segments(delta_day, season_boundaries)
		)

	return rolled
