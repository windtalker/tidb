# Master Port 非 MV 改动审计

本文档记录 source diff 中“看起来不是 MV 专属”的改动分类。

Source range:

```text
xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
```

本文档的目标不是决定最终实现细节，而是先回答两个问题：

- 哪些非 MV 改动很可能只是 branch drift，不应主动带到 `master` 的 MV port 分支。
- 哪些非 MV 改动虽然名字上不带 MV，但实际是 MV 的 prerequisite，不能直接丢。

总进度和 slice 状态仍然记录在：

```text
docs/note/materialized_view/mv_master_port_tracking.md
```

## 分类原则

- 不按文件名直接排除。非 MV 文件可能包含 MV prerequisite。
- 不按文件整体判断。混合文件必须按 hunk 级别审计。
- 如果 `master` 已有等价能力，port 时只适配当前 API，不重复引入 source diff 中的旧实现。
- build metadata、generated file、test result 最终应基于 master 重新生成，不直接照搬 source diff。

## 初步结论

### 已确认不 cp 的非 MV drift

这些改动已经过人工确认，不作为 master MV port 内容直接 cp。若后续 `master`
已经包含其中某些能力，MV 代码可以适配 master 现有实现；如果没有，也不在本次
MV port 中补这些无关改动。

| 类别 | 主要路径 | 内容 | 确认决策 |
| --- | --- | --- | --- |
| agent / 开发流程文档 | `AGENTS.md`、`docs/agents/*` | agent 工作规范、测试流程、架构索引等 | 不 cp |
| `.gitignore` 整理 | `.gitignore` | 大范围重排和补充本地工具、Bazel、测试产物 ignore 项 | 不 cp |
| build helper | `build/detect_base_branch.sh`、`build/get_changed_bazel_pkgs.sh` | 自动找 base branch、找变更 Bazel package 的脚本 | 不 cp |
| `cmd/mirror` 简化 | `cmd/mirror/*` | 去掉 GCS mirror/upload 和 skylark 解析，改为直接通过 GOPROXY 解析模块 | 不 cp |
| `google/skylark` 依赖删除 | `go.mod`、`go.sum` | 随 `cmd/mirror` 简化删除依赖 | 不 cp |
| OWNERS / Makefile / root metadata | `OWNERS`、`OWNERS_ALIASES`、`Makefile`、部分 `DEPS.bzl` | 仓库级 drift | 不 cp |
| TopSQL network bytes | `pkg/util/topsql/*`、`pkg/server/conn.go`、`pkg/executor/adapter.go` 的一部分 | 统计 network in/out bytes，并按 CPU/network 选择 TopN | 不 cp |
| prepare dedup / plan cache 优化 | `pkg/planner/core/plan_cache_utils.go`、`pkg/session/session.go`、`pkg/session/test/common/prepare_dedup_cache_test.go` | session 级 prepare dedup cache、plan cache key buffer 预估/复用 | 不 cp |
| active-active commit TS 独立测试 | `tests/integrationtest/t/active_active/commit_ts.test`、对应 result | `_tidb_commit_ts` user-visible 行为测试 | 不 cp；`_tidb_commit_ts` 能力本身仍按 MV prerequisite 单独审计 |
| 大范围 integration result churn | `tests/integrationtest/r/*` | planner/result 输出变化 | 不 cp source result；后续 port 到 master 后按实际行为重新录制 |

### 非 MV 名称，但属于 MV prerequisite candidate

这些改动不能简单排除。它们虽然不是 MV 语法或 MV service 本身，但 source 中
MV 实现或设计文档明确依赖这些能力。

| 类别 | 主要路径 | MV 依赖原因 | 初步决策 |
| --- | --- | --- | --- |
| FULL OUTER JOIN | `docs/note/fullouter_join_dev_note.md`、`pkg/parser/*`、`pkg/planner/core/*`、`pkg/executor/join/*`、`tests/integrationtest/*full_outer_join*` | `COMPARE MATERIALIZED VIEW` 和 `COMPLETE DELTA APPLY` 使用 FULL OUTER JOIN 作为 diff source | 作为 MV prerequisite 审计；如果 master 没有等价能力，需要 port 或替换实现 |
| `_tidb_commit_ts` / commit TS 下传 | `pkg/util/rowcodec/decoder.go`、`pkg/store/mockstore/unistore/*`、`tests/integrationtest/active_active/commit_ts.*` | MLog purge 和 fast refresh 依赖 `_tidb_commit_ts` 做 `(fromTS, targetTSO]` 过滤和 purge 边界 | `origin/master` 已有底层 commitTS 下传；SQL 层直接引用仍被禁用，MV port 需要单独处理可引用策略 |
| 新 aggregate 表达式 | `pkg/expression/aggregation/*`、`pkg/executor/aggfuncs/*`、`pkg/kv/checker.go`、`go.mod` 中 `tipb` bump | fast refresh count/sum/min/max 依赖 `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 等能力 | `origin/master` 已有 `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 的 parser、executor、expression、tipb pushdown 和 checker 支持；fast refresh 只适配现有实现 |
| chunk / serialization helper | `pkg/util/chunk/column.go`、`pkg/util/serialization/*` | 新 executor、agg spill、vector 类型处理可能依赖这些 helper | 逐个 hunk 审计；只带实际依赖 |
| BR system table restore | `br/pkg/restore/snap_client/systable_restore.go` 等 | MV system tables 使用 cluster-local table ID 和 TSO，restore 到其他集群没有语义；source 将其标为 unrecoverable | 作为 MV system table prerequisite port |
| `tipb` 版本升级 | `go.mod`、`go.sum`、`DEPS.bzl` | 可能提供 FullOuterJoin、SumInt、MaxCount、MinCount 等 pb 定义 | SumInt / MaxCount / MinCount 在 `origin/master` 当前 tipb 中已存在；FullOuterJoin 是否需要 tipb 支持仍随 FULL OUTER JOIN 审计 |
| `client-go` 版本升级 | `go.mod`、`go.sum`、`DEPS.bzl` | 可能和 commitTS / API V2 key decode / store 接口相关 | 需要确认 master 当前版本；必要时单独记录原因 |

## 混合文件

下面这些文件不能按文件整体 port 或整体排除，需要 hunk 级审计。

| 文件 / 区域 | 混合内容 | 处理方式 |
| --- | --- | --- |
| `pkg/executor/adapter.go` | MV affected rows metrics、MV purge slow log skip、refresh implement stmt slow log restore；同时包含 TopSQL network bytes | 只带 MV hunk；TopSQL hunk 单独决策 |
| `pkg/session/session.go` | MV service 启动、MV refresh/purge 禁止显式事务、internal session stats collector；同时包含 prepare dedup cache | 只带 MV hunk；prepare dedup 另行决策 |
| `pkg/session/test/common/common_test.go` | 新增 MV refresh/purge 行为测试 | 归入 MV tests，不按路径误判 |
| `pkg/sessionctx/variable/*` | MV sysvars、FULL OUTER JOIN sysvar、prepare cache 等通用变量 | 按变量逐个分类 |
| `pkg/planner/core/*` | MV planner hook / complete delta apply、FULL OUTER JOIN、plan cache key 优化、planner result churn | 按功能 slice 审计 |
| `tests/integrationtest/r/*` | MV 测试结果、FULL OUTER JOIN 结果、planner drift、aggregate pushdown drift | 不直接复制，按最终 port 后行为重新录制 |

## 需要进一步确认的点

| 问题 | 说明 | 状态 |
| --- | --- | --- |
| `master` 是否已有 FULL OUTER JOIN | 如果已有等价实现，MV 只需要适配；如果没有，需要决定是否把 FULL OUTER JOIN 作为前置 slice port | 待确认 |
| `master` 是否已有 `_tidb_commit_ts` 底层下传和 SQL 可引用支持 | `origin/master` 已有 `ExtraCommitTSID`、DataSource 隐藏列、table scan / MPP / rowcodec 的 commitTS 传递；但 preprocess 仍禁止 SELECT/UPDATE/DELETE/SET OPR 直接引用 `_tidb_commit_ts`，integration test 也仍期望报错。MV 后续若继续通过内部 SQL `WHERE _tidb_commit_ts ...` 做 purge/refresh，需要 port “允许引用”的最小改动，或改成只允许 internal/MLog 路径的 gate。 | 已确认：底层已有，SQL 可引用未启用 |
| `master` 的 `tipb` 是否已包含新 agg / join enum | `origin/master` 已包含 `tipb.ExprType_SumInt`、`tipb.ExprType_MaxCount`、`tipb.ExprType_MinCount`，并在 `agg_to_pb.go` / `kv/checker.go` 接入；FULL OUTER JOIN 相关 enum 仍需单独审计。 | 新 agg 已确认；join enum 待确认 |
| TopSQL network bytes 是否和 MV observability 有实际依赖 | 已确认不是本次 MV port 内容 | 不 cp |
| prepare dedup cache 是否为 MV 性能目标必需 | 已确认不是本次 MV port 内容 | 不 cp |

## 审计命令

筛出名字上不带 MV 的文件：

```bash
git diff --name-status xufei/cp_mv_for_master_base..xufei/cp_mv_for_master \
  | rg -v '(materialized|Materialized|mview|MView|mv_|mlog|MLog|MLOG|tidb_mview|tidb_mlog|TIDB_MVIEW|TIDB_MLOG)'
```

查看非 MV candidate 的 diff 概览：

```bash
git diff --stat xufei/cp_mv_for_master_base..xufei/cp_mv_for_master -- \
  br cmd build pkg/util/topsql pkg/executor/join \
  pkg/planner/core/casetest/fulljoin \
  tests/integrationtest/t/active_active tests/integrationtest/r/active_active \
  pkg/store/mockstore pkg/kv \
  pkg/session/test/common/prepare_dedup_cache_test.go \
  pkg/planner/core/plan_cache_utils.go
```

检查 FULL OUTER JOIN 是否被 MV 引用：

```bash
git grep -n -i 'full outer\|fullouter\|FullOuter' xufei/cp_mv_for_master -- \
  pkg/executor pkg/planner/mview pkg/ddl pkg/sessionctx pkg/parser \
  tests/integrationtest/t/executor/mview_refresh.test docs/note/materialized_view
```

检查 `_tidb_commit_ts` 是否被 MV 引用：

```bash
git grep -n '_tidb_commit_ts\|CommitTs\|commit ts\|DecodeToChunkWithCommitTS' \
  xufei/cp_mv_for_master -- \
  pkg/executor/materialized_view.go pkg/executor/mv_refresh_observability.go \
  pkg/planner/mview pkg/table/tables/mview_log.go \
  pkg/store pkg/util docs/note/materialized_view \
  tests/integrationtest/t/executor/mview_refresh.test
```

检查最新 `master` 的 `_tidb_commit_ts` 支持状态：

```bash
git fetch origin master --prune
git grep -n -C 10 'ExtraCommitTSName' origin/master -- \
  pkg/planner/core/preprocess.go \
  pkg/planner/core/logical_plan_builder.go \
  pkg/meta/model/table.go \
  pkg/util/rowcodec/decoder.go
git grep -n "Usage of column name '_tidb_commit_ts'\|select _tidb_commit_ts" \
  origin/master -- tests/integrationtest pkg/planner/core
```

检查 TopSQL network bytes 的引用面：

```bash
git grep -n 'OnExecutionBegin\|OnExecutionFinished\|NetworkInBytes\|NetworkOutBytes' \
  xufei/cp_mv_for_master -- \
  pkg/server pkg/session pkg/executor pkg/util/topsql pkg/mvservice docs/note/materialized_view
```

检查新 aggregate prerequisite 是否已在最新 `master`：

```bash
git grep -n 'AggFuncSumInt\|AggFuncMaxCount\|AggFuncMinCount' origin/master -- \
  pkg/parser/ast/functions.go \
  pkg/expression/aggregation/agg_to_pb.go \
  pkg/expression/aggregation/aggregation.go \
  pkg/executor/aggfuncs/builder.go \
  pkg/kv/checker.go
git grep -n 'ExprType_SumInt\|ExprType_MaxCount\|ExprType_MinCount' origin/master -- pkg
```

## 当前工作日志

| 日期 | 动作 | 结果 |
| --- | --- | --- |
| 2026-07-22 | 初步审计 source diff 中非 MV candidate | 形成三类：倾向排除、MV prerequisite candidate、混合文件需 hunk 级审计 |
| 2026-07-22 | 人工确认第一批非 MV drift 的 port 决策 | agent docs、`.gitignore`、build helper、`cmd/mirror`、`google/skylark` 删除、root metadata、TopSQL network bytes、prepare dedup / plan cache、active-active commit TS 独立测试均不 cp；integration result 后续重新录制 |
| 2026-07-22 | 对最新 `origin/master` 确认 `_tidb_commit_ts` 状态 | `master` 已有底层 commitTS 下传和隐藏列建模，但普通 SQL 直接引用仍被 preprocess 禁止；MV port 需要单独处理内部 SQL 使用 `_tidb_commit_ts` 的可引用策略 |
| 2026-07-22 | 对最新 `origin/master` 确认新 aggregate 状态 | `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 已有 parser / expression / executor / tipb pushdown / checker 支持；fast refresh 后续只适配，不重复 port 这些通用 aggregate |
