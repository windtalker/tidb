# Materialized View Specific TiDB Variables

This document lists the TiDB system variables that are introduced specifically for
materialized view related execution and maintenance.

It intentionally excludes MV service level parameters such as task scheduling,
backpressure, and history retention knobs.

## Summary

| Variable | Scope | Default | Purpose |
| --- | --- | --- | --- |
| `tidb_mlog_purge_batch_size` | `GLOBAL`, `SESSION` | `100000` | Batch size for `PURGE MATERIALIZED VIEW LOG` |
| `tidb_mv_maintain_mem_quota` | `GLOBAL`, `SESSION` | `2GB` | Memory quota for MV refresh / MV log purge internal maintenance sessions |
| `tidb_mv_maintain_isolation_read_engines` | `GLOBAL`, `SESSION` | `config.GetGlobalConfig().IsolationRead.Engines` | Isolation read engines for MV refresh / MV log purge internal maintenance sessions |
| `tidb_mview_maintain_import_threads` | `GLOBAL`, `SESSION` | `0` | `IMPORT INTO` thread count for MV initial build |
| `tidb_mview_maintain_import_disk_quota` | `GLOBAL`, `SESSION` | `''` | `IMPORT INTO` disk quota for MV initial build |

## `tidb_mlog_purge_batch_size`

- Scope: `GLOBAL`, `SESSION`
- Default: `100000`
- Range: `1` to `1000000`
- Purpose:
  controls how many rows each delete batch processes when executing
  `PURGE MATERIALIZED VIEW LOG`.

Example:

```sql
SET SESSION tidb_mlog_purge_batch_size = 50000;
PURGE MATERIALIZED VIEW LOG ON t;
```

## `tidb_mv_maintain_mem_quota`

- Scope: `GLOBAL`, `SESSION`
- Default: `2147483648` bytes (`2GB`)
- Range: `-1` to `9223372036854775807`
- Purpose:
  controls the memory quota used by MV refresh and MV log purge internal
  maintenance sessions.
- Notes:
  if the value is positive but smaller than `128`, TiDB truncates it to `128`
  and appends a warning.

Example:

```sql
SET GLOBAL tidb_mv_maintain_mem_quota = 8589934592;
```

## `tidb_mv_maintain_isolation_read_engines`

- Scope: `GLOBAL`, `SESSION`
- Default:
  the current TiDB configuration value of `isolation-read.engines`, formatted as
  a comma-separated engine list
- Purpose:
  controls which storage engines MV refresh and MV log purge internal
  maintenance sessions are allowed to read from.
- Validation:
  uses the same engine-list normalization and validation rules as
  `tidb_isolation_read_engines`.

Example:

```sql
SET SESSION tidb_mv_maintain_isolation_read_engines = 'tiflash';
```

## `tidb_mview_maintain_import_threads`

- Scope: `GLOBAL`, `SESSION`
- Default: `0`
- Range: `0` to `MaxConfigurableConcurrency`
- Purpose:
  controls the thread count used by the `IMPORT INTO` phase during MV initial
  build.
- Notes:
  `0` means MV does not add a `WITH thread=...` option when constructing the
  `IMPORT INTO` SQL. In that case, `IMPORT INTO` falls back to its own default.
  Since MV initial build uses `IMPORT INTO ... FROM (SELECT ...)`, the
  effective default thread count in this path is `2`.

Example:

```sql
SET SESSION tidb_mview_maintain_import_threads = 16;
```

## `tidb_mview_maintain_import_disk_quota`

- Scope: `GLOBAL`, `SESSION`
- Default: `''`
- Type: string
- Purpose:
  controls the disk quota used by the `IMPORT INTO` phase during MV initial
  build.
- Validation:
  empty string is allowed; non-empty values must be valid positive sizes that
  can be parsed by `units.RAMInBytes`, for example `10GiB`.
- Notes:
  `''` means MV does not add a `WITH disk_quota=...` option when constructing
  the `IMPORT INTO` SQL, so no disk quota is specified there.

Example:

```sql
SET SESSION tidb_mview_maintain_import_disk_quota = '200GiB';
```
