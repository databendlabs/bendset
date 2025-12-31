# /// script
# dependencies = ["pandas", "databend_driver"]
# ///

## Example:
## uv run bendset.py --dsn 'databend://root:@106.75.154.97:8000/?sslmode=disable' --start 2025-11-01 --end 2025-12-11

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd
from databend_driver import BlockingDatabendClient


PROFILE_SQL = """
SELECT
    timestamp,
    query_id,
    profiles,
    statistics_desc
FROM {db}.profile_history
WHERE timestamp >= '{start}' AND timestamp < '{end}'
"""

QUERY_SQL = """
SELECT
    query_id,
    query_start_time,
    event_time,
    query_queued_duration_ms,
    query_duration_ms,
    query_kind,
    log_type_name,
    scan_rows,
    scan_bytes,
    scan_io_bytes_cost_ms,
    written_rows,
    written_bytes,
    written_io_bytes,
    written_io_bytes_cost_ms,
    result_rows,
    result_bytes,
    peek_memory_usage,
    md5(current_database) as current_database,
    md5(sql_user) as sql_user
FROM {db}.query_history
WHERE event_time >= '{start}' AND event_time < '{end}'
"""

QUERY_COLUMNS = [
    "query_start_time",
    "event_time",
    "query_queued_duration_ms",
    "query_duration_ms",
    "query_kind",
    "log_type_name",
    "scan_rows",
    "scan_bytes",
    "scan_io_bytes_cost_ms",
    "written_rows",
    "written_bytes",
    "written_io_bytes",
    "written_io_bytes_cost_ms",
    "result_rows",
    "result_bytes",
    "peek_memory_usage",
    "current_database",
    "sql_user",
]

NUMERIC_QUERY_COLUMNS = [
    "query_queued_duration_ms",
    "query_duration_ms",
    "scan_rows",
    "scan_bytes",
    "scan_io_bytes_cost_ms",
    "written_rows",
    "written_bytes",
    "written_io_bytes",
    "written_io_bytes_cost_ms",
    "result_rows",
    "result_bytes",
]

DEFAULT_OUTPUT = Path(__file__).with_name("result.data")

STATISTIC_FIELDS = [
    ("cpu_time_sum", "CpuTime"),
    ("scan_bytes_from_local_disk_sum", "ScanBytesFromLocalDisk"),
    ("scan_bytes_from_data_cache_sum", "ScanBytesFromDataCache"),
    ("scan_bytes_from_remote_sum", "ScanBytesFromRemote"),
]

NODE_PATTERNS = [
    ("hash_join_nodes", "HashJoin"),
    ("aggregate_partial_nodes", "AggregatePartial"),
    ("sort_nodes", "Sort"),
]

OUTPUT_ROWS_KEY = "OutputRows"

METRIC_COLUMN_NAMES = (
    [name for name, _ in STATISTIC_FIELDS]
    + [name for name, _ in NODE_PATTERNS]
    + ["produced_rows"]
)

DEFAULT_STEP = "100w"
STEP_SUFFIX_MULTIPLIERS = {
    "kw": 1000 * 10000,
    "w": 10000,
    "m": 1_000_000,
    "k": 1000,
}


def parse_variant_field(value: Any) -> Any:
    """Normalize a VARIANT column into Python objects."""

    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")

    text = str(value).strip()
    if not text or text.upper() == "NULL":
        return None

    def _load_json(payload: str) -> Any:
        return json.loads(payload)

    try:
        parsed = _load_json(text)
    except json.JSONDecodeError:
        # Handle strings that double escape quotes when exported as TSV.
        if text.startswith('"') and text.endswith('"') and len(text) >= 2:
            inner = text[1:-1].replace('""', '"')
            try:
                parsed = _load_json(inner)
            except json.JSONDecodeError:
                return None
        else:
            return None
    if isinstance(parsed, str):
        try:
            return _load_json(parsed)
        except json.JSONDecodeError:
            return parsed
    return parsed


def _safe_number(value: Any) -> float:
    if isinstance(value, (int, float)):
        result = float(value)
        if isinstance(result, float) and math.isnan(result):
            return 0.0
        return result
    if value is None:
        return 0.0
    try:
        result = float(value)
        if math.isnan(result):
            return 0.0
        return result
    except (TypeError, ValueError):
        return 0.0


def sum_peek_memory_usage(value: Any) -> float:
    parsed = parse_variant_field(value)
    if isinstance(parsed, dict):
        return float(sum(_safe_number(v) for v in parsed.values()))
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes)):
        return float(sum(_safe_number(v) for v in parsed))
    return _safe_number(parsed)

def cal_node_num(value: Any) -> float:
    parsed = parse_variant_field(value)
    if isinstance(parsed, dict):
        return len(parsed.values())
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes)):
        return len(parsed.values())
    return 1


def parse_step_value(value: Any) -> int:
    if isinstance(value, (int, float)):
        result = int(value)
    else:
        text = str(value).strip()
        if not text:
            raise SystemExit("--step cannot be empty")
        lowered = text.lower()
        multiplier = 1
        for suffix in sorted(STEP_SUFFIX_MULTIPLIERS, key=len, reverse=True):
            if lowered.endswith(suffix):
                multiplier = STEP_SUFFIX_MULTIPLIERS[suffix]
                lowered = lowered[: -len(suffix)]
                break
        number_text = lowered.strip() or "1"
        number_text = number_text.replace("_", "")
        try:
            base_value = float(number_text)
        except ValueError as exc:
            raise SystemExit(f"Invalid --step value: {value}") from exc
        result = int(base_value * multiplier)
    if result <= 0:
        raise SystemExit("--step must be a positive number")
    return result


def chunk_output_path(base: Path, index: int) -> Path:
    return base.with_name(f"{base.name}_{index}")


def flush_frames(frames: List[pd.DataFrame], output: Path, chunk_index: int) -> int:
    if not frames:
        return chunk_index
    combined = pd.concat(frames, ignore_index=True)
    chunk_path = chunk_output_path(output, chunk_index)
    combined.to_csv(chunk_path, index=False, sep=',', encoding='utf-8')
    print(f"Saved {len(combined)} joined rows to {chunk_path}")
    frames.clear()
    return chunk_index + 1

def aggregate_profile_metrics(profiles_raw: Any, stats_desc_raw: Any) -> Dict[str, float]:
    metrics: Dict[str, float] = {name: 0.0 for name, _ in STATISTIC_FIELDS}
    metrics.update({name: 0.0 for name, _ in NODE_PATTERNS})
    metrics["produced_rows"] = 0.0

    profiles = parse_variant_field(profiles_raw) or []
    stats_desc = parse_variant_field(stats_desc_raw) or {}

    if not isinstance(profiles, list) or not isinstance(stats_desc, dict):
        return metrics

    index_lookup = {}
    for key, entry in stats_desc.items():
        if isinstance(entry, dict):
            idx = entry.get("index")
        else:
            idx = None
        if isinstance(idx, int):
            index_lookup[key] = idx

    for metric_name, stat_key in STATISTIC_FIELDS:
        idx = index_lookup.get(stat_key)
        if idx is None:
            continue
        total = 0.0
        for node in profiles:
            stats = node.get("statistics") if isinstance(node, dict) else None
            if not isinstance(stats, Sequence) or isinstance(stats, (str, bytes)):
                continue
            if idx < len(stats):
                total += _safe_number(stats[idx])
        metrics[metric_name] = total

    for metric_name, keyword in NODE_PATTERNS:
        count = 0
        for node in profiles:
            name = node.get("name") if isinstance(node, dict) else None
            if isinstance(name, str) and keyword in name:
                count += 1
        metrics[metric_name] = float(count)

    root_idx = index_lookup.get(OUTPUT_ROWS_KEY)
    if root_idx is not None:
        for node in profiles:
            if not isinstance(node, dict):
                continue
            parent_id = node.get("parent_id")
            if parent_id not in (None, "null", "NULL"):
                continue
            stats = node.get("statistics")
            if not isinstance(stats, Sequence) or isinstance(stats, (str, bytes)):
                continue
            if root_idx < len(stats):
                metrics["produced_rows"] = _safe_number(stats[root_idx])
                break

    return metrics


def fetch_dataframe(conn, sql: str, columns: List[str]) -> pd.DataFrame:
    rows = conn.query_iter(sql)
    data = [row.values() for row in rows]
    if not data:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(data, columns=columns)


def _format_ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def compose_sql(template: str, db: str, start: datetime, end: datetime) -> str:
    return template.format(db = db, start=_format_ts(start), end=_format_ts(end))


def compose_join_sql(db: str, start: datetime, end: datetime) -> str:
    profile_sql = compose_sql(PROFILE_SQL, db, start, end)
    query_sql = compose_sql(QUERY_SQL, db, start, end)
    return f"""
WITH profile_data AS (
{profile_sql}
),
query_data AS (
{query_sql}
)
SELECT
    profile_data.query_id,
    profile_data.profiles,
    profile_data.statistics_desc,
    query_data.query_start_time,
    query_data.event_time,
    query_data.query_queued_duration_ms,
    query_data.query_duration_ms,
    query_data.query_kind,
    query_data.log_type_name,
    query_data.scan_rows,
    query_data.scan_bytes,
    query_data.scan_io_bytes_cost_ms,
    query_data.written_rows,
    query_data.written_bytes,
    query_data.written_io_bytes,
    query_data.written_io_bytes_cost_ms,
    query_data.result_rows,
    query_data.result_bytes,
    query_data.peek_memory_usage,
    query_data.current_database,
    query_data.sql_user
FROM profile_data
INNER JOIN query_data ON profile_data.query_id = query_data.query_id
ORDER BY query_data.event_time DESC
"""


def fetch_joined_history(
    conn,
    db: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    sql = compose_join_sql(db, start, end)
    columns = ["query_id", "profiles", "statistics_desc"] + QUERY_COLUMNS
    return fetch_dataframe(conn, sql, columns)


def build_result_dataframe(
    conn,
    db,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    joined_df = fetch_joined_history(conn, db, start, end)
    if joined_df.empty:
        return pd.DataFrame()

    if "event_time" in joined_df.columns:
        joined_df = joined_df.sort_values("event_time")

    results: Dict[str, Dict[str, Any]] = {}
    metric_keys = list(METRIC_COLUMN_NAMES)

    for _, row in joined_df.iterrows():
        query_id = row["query_id"]
        entry = results.get(query_id)
        if entry is None:
            entry = {"query_id": query_id}
            for key in metric_keys:
                entry[key] = 0.0
            entry["node_num"] = 0.0
            results[query_id] = entry

        metrics = aggregate_profile_metrics(row["profiles"], row["statistics_desc"])
        for key, value in metrics.items():
            entry[key] = entry.get(key, 0.0) + _safe_number(value)

        for column in QUERY_COLUMNS:
            if column == "peek_memory_usage":
                continue
            value = row[column]
            if pd.isna(value):
                value = None
            if column in NUMERIC_QUERY_COLUMNS:
                value = _safe_number(value)
            entry[column] = value

        if "peek_memory_usage" in joined_df.columns:
            peek_value = row["peek_memory_usage"]
            if pd.isna(peek_value):
                peek_value = None
            peek_sum = sum_peek_memory_usage(peek_value)
            entry["peek_memory_usage"] = peek_sum
            entry["node_num"] = cal_node_num(peek_value)

    ordered_columns = (
        ["query_id"]
        + metric_keys
        + QUERY_COLUMNS
        + ["node_num"]
    )
    final_df = pd.DataFrame(results.values())
    final_df = final_df.reindex(columns=ordered_columns)
    return final_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate Databend profile statistics.")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABEND_DSN"),
        help="Databend DSN, e.g. databend://user:pass@host:8000/?sslmode=disable",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to the TSV file that will store the merged result",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--db",
        required=False,
        default='system_history',
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--debug",
        action='store_true',
        help="debug or not",
    )
    parser.add_argument(
        "--step",
        default=DEFAULT_STEP,
        help="Row threshold per output file chunk (e.g. 500000, 1w, 1kw)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dsn:
        raise SystemExit("Databend DSN is required. Provide --dsn or set DATABEND_DSN.")

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid date format: {exc}") from exc

    if start_date > end_date:
        raise SystemExit("--start must be earlier than or equal to --end")

    step_threshold = parse_step_value(args.step)
    if args.debug:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

    client = BlockingDatabendClient(args.dsn)
    conn = client.get_conn()

    frames: List[pd.DataFrame] = []
    chunk_index = 0
    pending_rows = 0
    total_rows = 0

    range_start = datetime.combine(start_date, datetime.min.time())
    range_end = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
    current_time = range_start

    print(f"Step threshold per output file: {step_threshold} rows")

    while current_time < range_end:
        hour_start = current_time
        hour_end = min(hour_start + timedelta(hours=1), range_end)
        db = args.db

        print(f"[start]analyze logs from {hour_start} to {hour_end}")
        df = build_result_dataframe(
            conn,
            db,
            hour_start,
            hour_end,
        )
        print(f"[end]analyze logs from {hour_start} to {hour_end}, rows: {len(df)}")
        if not df.empty:
            frames.append(df)
            rows = len(df)
            pending_rows += rows
            total_rows += rows
            if pending_rows >= step_threshold:
                chunk_index = flush_frames(frames, args.output, chunk_index)
                pending_rows = 0
        current_time = hour_end

    if total_rows == 0 and not frames and chunk_index == 0:
        print("No data found for the requested history tables.")
        return

    if frames:
        chunk_index = flush_frames(frames, args.output, chunk_index)

    print(
        f"Finished writing {total_rows} rows across {chunk_index} file(s) "
        f"with base {args.output.name}"
    )


if __name__ == "__main__":
    main()


