# Materialized View User-Visible SQL Syntax Summary

This note summarizes the user-facing SQL syntax defined or proposed in:

- `docs/note/materialized_view/mv_refresh.md`
- `docs/note/materialized_view/kill_refresh_purge.md`

It is intentionally short and focuses only on syntax and user-visible semantics.
Implementation details stay in the original design notes.

## Refresh Syntax Available Today

### Complete refresh

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE IN PLACE;
REFRESH MATERIALIZED VIEW mv COMPLETE OUT OF PLACE;
REFRESH MATERIALIZED VIEW mv COMPLETE DELTA APPLY;
```

Notes:

- `COMPLETE IN PLACE` is full table replacement.
- `COMPLETE OUT OF PLACE` means shadow-table build plus cutover.
- `COMPLETE DELTA APPLY` means diff-and-apply refresh instead of full table replacement.
- `REFRESH MATERIALIZED VIEW mv COMPLETE` uses the current database if schema is omitted.
- if the complete type is omitted, `REFRESH MATERIALIZED VIEW mv COMPLETE` is equivalent to `REFRESH MATERIALIZED VIEW mv COMPLETE DELTA APPLY`.

### Fast refresh

```sql
REFRESH MATERIALIZED VIEW mv FAST;
```

Notes:

- `FAST` refresh currently requires `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` to be non-`NULL`.

### Async syntax reservation

The following syntax is parsed, but the executor currently rejects it because async refresh is not implemented yet:

```sql
REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE COMPLETE;
REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE FAST;
```

## Additional Refresh Syntax Available Today

### Bounded fast refresh

The user-facing syntax is:

```sql
REFRESH MATERIALIZED VIEW mv FAST AS OF TIMESTAMP <expr>;
```

Semantics:

- this syntax is only for `FAST` refresh
- `<expr>` defines the refresh upper bound
- after success, `LAST_SUCCESS_READ_TSO` should advance to that target timestamp

Explicit non-goal for this syntax:

- `AS OF TIMESTAMP` is not meant to make the whole refresh statement behave like a generic stale-read statement

## Cancel Syntax Available Today

Current syntax supports cancel by job id.

### Cancel one refresh job

```sql
CANCEL MATERIALIZED VIEW REFRESH JOB <job_id>;
```

### Cancel one purge job

```sql
CANCEL MATERIALIZED VIEW LOG PURGE JOB <job_id>;
```

Intended semantics:

- `<job_id>` identifies one concrete running attempt
- the statement only succeeds for a live running job
- cancel is expected to work for both:
  - manually submitted refresh/purge
  - auto-triggered refresh/purge from `mvservice`

`job_id` comes from the corresponding history table:

- refresh: `mysql.tidb_mview_refresh_hist.REFRESH_JOB_ID`
- purge: `mysql.tidb_mlog_purge_hist.PURGE_JOB_ID`

Each refresh or purge attempt writes one history row for that run, and the `job_id` is the identifier of that history row.

## Quick Checklist

Already supported in refresh:

- `COMPLETE`
- `COMPLETE IN PLACE`
- `COMPLETE OUT OF PLACE`
- `COMPLETE DELTA APPLY`
- `FAST`
- `FAST AS OF TIMESTAMP <expr>`

Already supported in cancel:

- `CANCEL MATERIALIZED VIEW REFRESH JOB <job_id>`
- `CANCEL MATERIALIZED VIEW LOG PURGE JOB <job_id>`
