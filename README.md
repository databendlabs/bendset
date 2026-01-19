# Bendset Dataset

This repository documents Bendset, an anonymized Databend query workload released to make it easier to study large-scale, cloud data warehouse behavior. The dataset captures 39 million production queries executed between **2025‑12‑09** and **2025‑12‑11** and ships with scripts to reproduce the curated CSV files or generate new slices directly from your own Databend history tables.

## Overview
- **Public snapshot**: https://sharing.databend.cloud/bendset/bendset-2025-12-11.tar.gz (39 M queries, CSV chunks).
- **Source tables**: `system_history.query_history` and `system_history.profile_history`, joined per query to keep execution profiles next to user-visible statistics.
- **Granularity**: one row per query with timing, I/O, resource usage, queueing, result sizes, anonymized database/user identifiers, and profile-derived operator metrics.
- **License**: see `LICENSE`.

## Quick Start
1. Download and extract the latest archive.
   ```bash
   curl -LO https://sharing.databend.cloud/bendset/bendset-2025-12-11.tar.gz
   tar -xzf bendset-2025-12-11.tar.gz
   ```
2. (Optional) Regenerate a fresh slice from your Databend history service:
   ```bash
   export DATABEND_DSN=$YOUR_DATABEND_DSN
   uv run bendset.py --dsn "$DATABEND_DSN" --start 2025-12-09 --end 2025-12-11
   ```
   The script streams hourly windows, joins query and profile history, and writes CSV chunks such as `result.data_0`, `result.data_1`, … (controlled by `--output` and `--step`).

## Main Dataset
The main CSV files contain one record per query ordered by `event_time`. Alongside the raw identifiers (`query_id`, `current_database`, `sql_user`, all hashed), each row includes:
- **Timing**: `query_start_time`, `event_time`, `query_queued_duration_ms`, `query_duration_ms`.
- **Classification**: `query_kind`, `query_hash`, `query_parameterized_hash`, `log_type_name`.
- **Scan/Write volume**: `scan_rows`, `scan_bytes`, `scan_io_bytes_cost_ms`, `written_rows`, `written_bytes`, `written_io_bytes`, `written_io_bytes_cost_ms`.
- **Results**: `result_rows`, `result_bytes`.
- **Resource tracking**: `peek_memory_usage` (aggregated across fragments) and `node_num` (number of fragments/operators observed).
- **Profile aggregates** pulled from `profile_history`: `cpu_time_sum`, `scan_bytes_from_local_disk_sum`, `scan_bytes_from_data_cache_sum`, `scan_bytes_from_remote_sum`, `hash_join_nodes`, `aggregate_partial_nodes`, `sort_nodes`, and `produced_rows`.

The `query_id` column uniquely identifies each query and serves as the join key when combining the dataset with other telemetry.

## Sample Data
The `example.csv` file in this repository contains the first few rows of a processed slice and mirrors the layout of the public snapshot. A short excerpt is shown below:

```csv
query_id,cpu_time_sum,scan_bytes_from_local_disk_sum,scan_bytes_from_data_cache_sum,scan_bytes_from_remote_sum,hash_join_nodes,aggregate_partial_nodes,sort_nodes,produced_rows,query_start_time,event_time,query_queued_duration_ms,query_duration_ms,query_kind,query_hash,query_parameterized_hash,log_type_name,scan_rows,scan_bytes,scan_io_bytes_cost_ms,written_rows,written_bytes,written_io_bytes,written_io_bytes_cost_ms,result_rows,result_bytes,peek_memory_usage,current_database,sql_user,node_num
019b03d74a747df3a4d250ebb7ef43c1,29116853.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,2025-12-09 15:59:59.620594+00:00,2025-12-09 16:00:00.007417+00:00,0.0,386.0,CopyIntoTable,,,Finish,278.0,152064.0,0.0,278.0,786253.0,6389.0,0.0,0.0,0.0,263573309.0,c21f969b5f03d33d43e04f8f136e7682,269c24d5505ad4801e3238c586a1f52c,1
70010100-95eb-4a5a-8e75-4d4557780776,2572887.0,0.0,0.0,0.0,2.0,0.0,0.0,0.0,2025-12-09 15:59:59.199122+00:00,2025-12-09 16:00:00.009906+00:00,518.0,810.0,Query,,,Finish,1123.0,125183.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,21085030.0,c21f969b5f03d33d43e04f8f136e7682,1eefadf0ae4d5031dae553197fba763f,1
```

Use this file as a quick sanity check for downstream pipelines or to verify that parsers interpret timestamps, floats, and hashes correctly.

## Column Reference
| Column | Description |
| --- | --- |
| `query_id` | Unique identifier for every query log entry (UUID or hashed) and join key across tables. |
| `cpu_time_sum` | Total CPU time consumed by the query as reported by aggregated plan-node statistics. |
| `scan_bytes_from_local_disk_sum` | Bytes read from local disk caches. |
| `scan_bytes_from_data_cache_sum` | Bytes served from Databend’s data cache tier. |
| `scan_bytes_from_remote_sum` | Bytes scanned from remote object storage. |
| `hash_join_nodes` | Count of plan nodes whose name contains `HashJoin`, a proxy for join complexity. |
| `aggregate_partial_nodes` | Count of partial aggregation nodes encountered in the profile. |
| `sort_nodes` | Count of sort operators observed. |
| `produced_rows` | Rows produced by the root node (matches `OutputRows` in the profile when available). |
| `query_start_time` | UTC wall-clock timestamp when the query started running. |
| `event_time` | UTC timestamp when the history event was recorded (typically completion time). |
| `query_queued_duration_ms` | Milliseconds spent waiting in the scheduler before execution. |
| `query_duration_ms` | Milliseconds spent executing (between start and finish). |
| `query_kind` | Statement category such as `Query`, `CopyIntoTable`, etc. |
| `query_parameterized` | Parameterized version of the query (with literal values replaced by placeholders). |
| `query_parameterized_hash` | Hash of the parameterized query for grouping similar queries. |
| `log_type_name` | Lifecycle stage captured by the history row (e.g., `Finish`, `Exception`). |
| `scan_rows` | Total rows scanned from storage. |
| `scan_bytes` | Total bytes scanned from storage. |
| `scan_io_bytes_cost_ms` | I/O cost estimate in milliseconds for scanning (as reported by history tables). |
| `written_rows` | Rows persisted to storage by the query. |
| `written_bytes` | Bytes written to storage. |
| `written_io_bytes` | Bytes transferred during write I/O operations. |
| `written_io_bytes_cost_ms` | I/O cost estimate (ms) for writes. |
| `result_rows` | Rows returned to the client. |
| `result_bytes` | Bytes returned to the client. |
| `peek_memory_usage` | Sum of per-fragment peak memory usage, derived from the VARIANT profile data. |
| `current_database` | MD5 hash of the originating database name for privacy. |
| `sql_user` | MD5 hash of the authenticated SQL user. |
| `node_num` | Number of fragments/operators recorded for the query’s execution. |

## Schema Notes
- Every column in the exported CSV matches the names produced by `bendset.py`. Numeric fields are normalized to floating point numbers to avoid `NaN` drift.
- `peek_memory_usage` is derived from the VARIANT column in `query_history` by summing the per-fragment peaks.
- `produced_rows` reflects the output cardinality of the root plan node, when the profile reports `OutputRows`.
- Timestamps are UTC, formatted as `%Y-%m-%d %H:%M:%S`.

## Profile Time-Series Expansion
While the main dataset has aggregated per-query metrics, you can regenerate a time-series expansion by re-running `bendset.py` with `--debug` and post-processing the `profiles` column before aggregation. Each profile entry consists of `(timestamp, query_id)` pairs describing which queries were active at a given instant, allowing analyses such as concurrent query counts or CPU usage envelopes.

## Scripts
- `bendset.py` (default entry point): connects via `databend_driver`, reads hourly windows from history tables, aggregates operator statistics, and emits CSV chunks. Key options: `--dsn`, `--start`, `--end`, `--db` (defaults to `system_history`), `--output`, `--step`, and `--debug`.
- `main.py`: lightweight placeholder used when distributing the package; real workloads should invoke `bendset.py`.

## Limitations & Privacy
- **Sampling window**: the public snapshot only covers three days. Use the script with your own DSN to fetch longer windows.
- **Privacy**: user-facing identifiers (`current_database`, `sql_user`) are MD5-hashed to preserve anonymity. Query text is not included.
- **Schema drift**: Future Databend releases may add or rename history columns. Re-run the script against your target version to regenerate consistent data.

Please open an issue or PR if you discover inconsistencies or have suggestions for additional derived metrics.
