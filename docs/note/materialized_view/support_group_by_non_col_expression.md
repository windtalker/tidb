# Support `GROUP BY` Non-Column Expressions in Materialized Views

This note captures the implementation plan for supporting materialized view definitions such as:

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT col1, DATE(col2) AS d, COUNT(*)
FROM t
GROUP BY col1, DATE(col2);
```

## Background

Current MV implementation only supports grouped MVs whose `GROUP BY` items are plain column names.

The current restriction is enforced in two places:

1. DDL validation for `CREATE MATERIALIZED VIEW` only accepts:
   - `GROUP BY` items that are `ColumnNameExpr`
   - non-aggregate `SELECT` items that are also plain columns
2. `mvmerge` currently models group keys as:
   - offsets in MV output schema
   - plus an implicit assumption that each group key can be mapped back to one base-table column name

That second assumption is the main blocker for `FAST` refresh and `MIN/MAX` full-update fallback.

## Why We Should Not Use the Generated-Column Workaround

One possible workaround is:

1. add a generated column on the base table, for example `g DATE AS (DATE(col2)) VIRTUAL`
2. create an index on `(col1, g)`
3. define the MV as `GROUP BY col1, g`

This workaround is not acceptable for the target use case.

Reason:

- `COMPLETE` refresh and init build must preserve the existing TiFlash-oriented bulk-query path
- virtual generated columns can still appear in a TiFlash plan, but pushdown quality is weaker
- TiDB currently does not support adding a stored generated column through `ALTER TABLE`
- therefore, for existing base tables, the workaround would realistically rely on virtual generated columns
- this introduces unacceptable risk for init build / complete refresh performance

So the target direction is to support `GROUP BY` expressions natively in MV definition, while keeping:

- bulk rebuild paths (`CREATE MATERIALIZED VIEW` init build, `COMPLETE IN PLACE`, `COMPLETE OUT OF PLACE`) on the original MV query shape
- keyed fallback paths (`FAST` refresh with `MIN/MAX`) on TiKV index access, including expression index

## Scope

### In scope

Support MV definitions where:

- the MV is still a single-base-table grouped MV
- each `GROUP BY` expression also appears in the `SELECT` list
- `FAST`, `COMPLETE DELTA APPLY`, `COMPLETE IN PLACE`, and `COMPLETE OUT OF PLACE` all work with such definitions
- `FAST` refresh with `MIN/MAX` is supported when the base table has a usable covering key layout, including expression index such as `(col1, DATE(col2))`

### Out of scope for the first implementation

- `GROUP BY` ordinal position syntax
- `GROUP BY` alias-only matching
- relaxing existing aggregate restrictions unrelated to group-key representation
- multi-table MV definitions
- introducing a generated-column-based compatibility path

## Design Principles

1. Keep the logical identity of a grouped MV row as the MV output group key.
2. Keep bulk rebuild SQL unchanged so init build / complete refresh can still use the original query and preserve TiFlash/MPP opportunities.
3. Rework incremental refresh to treat group keys as general expressions, not just base-column names.
4. Keep the executor-side sink contract stable whenever possible:
   - `GroupKeyMVOffsets` should remain the primary group-key identity
   - downstream data-change application should continue to work by MV output offsets
5. For `MIN/MAX` fallback, prefer validating by actual lookup-template optimization result rather than only by static column-name rules.

## Functional Requirements

For a grouped MV with non-column group keys:

1. `CREATE MATERIALIZED VIEW` should accept deterministic group expressions that also appear in the `SELECT` list.
2. MV log validation should require all base columns referenced by:
   - group-key expressions
   - aggregate arguments
   - MV `WHERE` clause
   to be present in the MLog tracked-column set.
3. `COMPLETE DELTA APPLY` should continue to use MV output columns as diff-join keys.
4. `FAST` refresh stage-1 delta aggregation should be able to aggregate MLog rows by the same group expressions.
5. `FAST` refresh `MIN/MAX` full-update fallback should be able to rebuild changed groups by expression keys and hit expression index when available.

## Development Plan

### Phase 1: DDL Validation and MV Definition Metadata

Goal:

- accept MV definitions with `GROUP BY` expressions
- keep MV metadata modeled by MV output position rather than base-column name

Tasks:

1. Relax `CREATE MATERIALIZED VIEW` validation:
   - allow `GROUP BY` expressions instead of only `ColumnNameExpr`
   - allow matching non-aggregate `SELECT` expressions instead of only plain columns
2. Require each `GROUP BY` expression to appear in the `SELECT` list exactly once for v1.
3. Match `GROUP BY` items to `SELECT` items by canonical expression equality after:
   - cloning
   - stripping table qualifiers
   - restoring to canonical SQL form
4. Rework `groupByInfos` derivation:
   - keep `SelectIdx`
   - derive `NotNull` from the query output field / MV output column metadata, not only from base column flags
5. Extend used-column collection so group-key expressions contribute referenced base columns to the MLog dependency check.

Expected result:

- MV definition can be created with `GROUP BY col1, DATE(col2)`
- MV physical columns are still derived from the original query output
- no generated-column workaround is required

### Phase 2: Shared Group-Key Expression Helpers in `mvmerge`

Goal:

- make group-key extraction expression-based instead of column-name-based

Tasks:

1. Replace `extractGroupKeyOffsetsFromMVSelect` with expression-aware matching.
2. Introduce helpers that can return, by MV output offset:
   - cloned group-key expression
   - stripped/unqualified group-key expression
3. Remove the assumption that a group key must map to one base column name.
4. Keep `GroupKeyMVOffsets` as the executor-visible contract.

Expected result:

- planner metadata for group keys becomes expression-safe
- `COMPLETE DELTA APPLY` can reuse the existing offset-based sink contract with minimal executor change

### Phase 3: `COMPLETE DELTA APPLY`

Goal:

- ensure complete diff path works with group-key expressions

Tasks:

1. Reuse the new expression-aware group-key offset extraction.
2. Keep diff join keyed by MV output columns from:
   - query side (`Q`)
   - current MV side (`M`)
3. Preserve existing behavior for:
   - side-missing marker
   - handle columns
   - payload comparison

Expected result:

- `COMPLETE DELTA APPLY` should need only small planner changes because it already joins by MV output columns

### Phase 4: `FAST` Refresh Stage-1 Delta Aggregation

Goal:

- aggregate MLog rows by group expressions, not only by base-column names

Tasks:

1. In `buildMLogDeltaSelect`, build group-key projection and `GROUP BY` items from cloned group expressions.
2. Strip table qualifiers so expressions can be evaluated on the MLog schema.
3. Keep aggregate delta computation unchanged where possible.
4. Ensure every base column referenced by group expressions is required to exist in the MLog tracked-column set.

Expected result:

- `FAST` refresh without `MIN/MAX` should work for group-key expressions such as `DATE(col2)`

### Phase 5: `FAST` Refresh `MIN/MAX` Full-Update Fallback

Goal:

- support expression-key lookup for changed groups
- allow expression index such as `(col1, DATE(col2))`

Tasks:

1. Rework full-update lookup template builders so outer/inner key expressions are general group expressions, not just column names.
2. Build outer probe key tuple from group-key expressions.
3. Build inner grouped recomputation by the same expressions.
4. Rework key-to-result mapping so it is no longer tied to base-column-name extraction.
5. Replace or narrow the current static validator:
   - keep current static column-name validator for pure-column group keys if desired
   - for expression group keys, validate by actual optimized lookup template result
6. The plan-based success criteria for expression-key fallback should be:
   - optimizer produces the expected lookup/index-join style template
   - key/range mapping can be extracted
   - runtime executor can rebuild changed groups using that template

Expected result:

- `FAST` refresh with `MIN/MAX` works when a usable expression index exists
- fallback path stays aligned with its current TiKV-index-oriented design

## Validation Strategy

### DDL tests

Add/extend tests for:

- allowed case:
  - `SELECT col1, DATE(col2), COUNT(*) ... GROUP BY col1, DATE(col2)`
- rejected case:
  - group expression not present in `SELECT`
  - duplicate matching `SELECT` expressions
  - unsupported/dangerous syntax kept out of scope for v1
- MLog dependency checks:
  - missing base column referenced by group expression should be rejected

### Planner / `mvmerge` tests

Add/extend tests for:

- group-key offset extraction by expression match
- `COMPLETE DELTA APPLY` diff-source generation with group expressions
- `FAST` stage-1 delta aggregation SQL for group expressions
- `FAST` `MIN/MAX` full-update lookup template generation with expression keys

### Executor / refresh tests

Add/extend tests for:

- complete refresh success with group-key expressions
- fast refresh success without `MIN/MAX`
- fast refresh with `MIN/MAX` and matching expression index
- failure path when required expression index is absent or lookup template cannot be optimized into the expected keyed path

## Risks and Open Points

### Expression equality

Main risk:

- matching `GROUP BY` items to `SELECT` items robustly

Preferred v1 approach:

- strip qualifiers
- restore to canonical SQL
- compare canonical form

### Nullability

For plain columns, nullability was derived from base-column metadata.

For expressions, nullability must come from expression output metadata instead.

This matters because:

- MV build decides whether to create `PRIMARY KEY` or `UNIQUE`
- diff join uses `=` for `NOT NULL` keys and `<=>` for nullable keys

### `MIN/MAX` index validation

Current implementation assumes a column-name prefix-covering index.

For expression keys, the implementation should not rely on a hand-written column-name validator alone.

Preferred direction:

- use actual lookup-template optimization success as the authoritative validation for expression-key fallback

### Performance regression

The main performance risk is not incremental lookup itself.

The main performance risk is accidentally degrading init build / complete refresh by replacing the original MV query shape with generated-column indirection.

This plan explicitly avoids that path.

## Recommended Implementation Order

1. Phase 1: DDL validation and metadata
2. Phase 2: shared expression-aware group-key helpers
3. Phase 3: `COMPLETE DELTA APPLY`
4. Phase 4: `FAST` without `MIN/MAX`
5. Phase 5: `FAST` with `MIN/MAX` fallback + expression index

This order keeps the bulk-query path stable first, then unlocks incremental paths in increasing difficulty.
