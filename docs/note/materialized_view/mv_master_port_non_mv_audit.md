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

### 倾向排除或确认 master 已有的非 MV drift

这些改动目前没有看到 MV 直接依赖。后续 port 到 `master` 时，优先判断
`master` 是否已经有等价实现；如果没有明确 MV 依赖，倾向不带入 MV port。

| 类别 | 主要路径 | 内容 | 初步决策 |
| --- | --- | --- | --- |
| agent / 开发流程文档 | `AGENTS.md`、`docs/agents/*` | agent 工作规范、测试流程、架构索引等 | 倾向排除；保留在本地 agent 文档即可 |
| `.gitignore` 整理 | `.gitignore` | 大范围重排和补充本地工具、Bazel、测试产物 ignore 项 | 倾向排除 |
| build helper | `build/detect_base_branch.sh`、`build/get_changed_bazel_pkgs.sh` | 自动找 base branch、找变更 Bazel package 的脚本 | 倾向排除，除非后续验证流程明确需要 |
| `cmd/mirror` 简化 | `cmd/mirror/*` | 去掉 GCS mirror/upload 和 skylark 解析，改为直接通过 GOPROXY 解析模块 | 倾向排除或单独处理，不作为 MV port 内容 |
| `google/skylark` 依赖删除 | `go.mod`、`go.sum` | 随 `cmd/mirror` 简化删除依赖 | 跟随 `cmd/mirror` 决策；不作为 MV prerequisite |
| OWNERS / Makefile / root metadata | `OWNERS`、`OWNERS_ALIASES`、`Makefile`、部分 `DEPS.bzl` | 仓库级 drift | 倾向排除；必要 metadata 应由 master 上的生成命令产生 |
| TopSQL network bytes | `pkg/util/topsql/*`、`pkg/server/conn.go`、`pkg/executor/adapter.go` 的一部分 | 统计 network in/out bytes，并按 CPU/network 选择 TopN | 倾向排除；目前未发现 MV 直接依赖 |
| prepare dedup / plan cache 优化 | `pkg/planner/core/plan_cache_utils.go`、`pkg/session/session.go`、`pkg/session/test/common/prepare_dedup_cache_test.go` | session 级 prepare dedup cache、plan cache key buffer 预估/复用 | 倾向排除或单独性能 PR，不作为 MV 语义依赖 |
| active-active commit TS 独立测试 | `tests/integrationtest/t/active_active/commit_ts.test`、对应 result | `_tidb_commit_ts` user-visible 行为测试 | 是否保留取决于 `_tidb_commit_ts` 能力是否随 MV prerequisite 一起 port |
| 大范围 integration result churn | `tests/integrationtest/r/*` | planner/result 输出变化 | 不直接照搬；按 port 后实际行为重新录制 |

### 非 MV 名称，但属于 MV prerequisite candidate

这些改动不能简单排除。它们虽然不是 MV 语法或 MV service 本身，但 source 中
MV 实现或设计文档明确依赖这些能力。

| 类别 | 主要路径 | MV 依赖原因 | 初步决策 |
| --- | --- | --- | --- |
| FULL OUTER JOIN | `docs/note/fullouter_join_dev_note.md`、`pkg/parser/*`、`pkg/planner/core/*`、`pkg/executor/join/*`、`tests/integrationtest/*full_outer_join*` | `COMPARE MATERIALIZED VIEW` 和 `COMPLETE DELTA APPLY` 使用 FULL OUTER JOIN 作为 diff source | 作为 MV prerequisite 审计；如果 master 没有等价能力，需要 port 或替换实现 |
| `_tidb_commit_ts` / commit TS 下传 | `pkg/util/rowcodec/decoder.go`、`pkg/store/mockstore/unistore/*`、`tests/integrationtest/active_active/commit_ts.*` | MLog purge 和 fast refresh 依赖 `_tidb_commit_ts` 做 `(fromTS, targetTSO]` 过滤和 purge 边界 | 作为 MV prerequisite 审计；测试位置可重新组织 |
| 新 aggregate 表达式 | `pkg/expression/aggregation/*`、`pkg/executor/aggfuncs/*`、`pkg/kv/checker.go`、`go.mod` 中 `tipb` bump | fast refresh count/sum/min/max 依赖 `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 等能力 | 跟随 fast refresh slice port |
| chunk / serialization helper | `pkg/util/chunk/column.go`、`pkg/util/serialization/*` | 新 executor、agg spill、vector 类型处理可能依赖这些 helper | 逐个 hunk 审计；只带实际依赖 |
| BR system table restore | `br/pkg/restore/snap_client/systable_restore.go` 等 | MV system tables 使用 cluster-local table ID 和 TSO，restore 到其他集群没有语义；source 将其标为 unrecoverable | 作为 MV system table prerequisite port |
| `tipb` 版本升级 | `go.mod`、`go.sum`、`DEPS.bzl` | 可能提供 FullOuterJoin、SumInt、MaxCount、MinCount 等 pb 定义 | 需要时在 master 上更新并重新生成 Bazel metadata |
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
| `master` 是否已有 `_tidb_commit_ts` cop/point-get 支持 | 如果已有，MLog purge/refresh 只适配；如果没有，需要 port commitTS 下传能力 | 待确认 |
| `master` 的 `tipb` 是否已包含新 agg / join enum | 影响是否需要 bump `tipb` | 待确认 |
| TopSQL network bytes 是否和 MV observability 有实际依赖 | 当前没有看到直接依赖 | 倾向排除 |
| prepare dedup cache 是否为 MV 性能目标必需 | 当前看起来是通用 prepare 性能优化，不是 MV correctness 依赖 | 倾向排除 |

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

检查 TopSQL network bytes 的引用面：

```bash
git grep -n 'OnExecutionBegin\|OnExecutionFinished\|NetworkInBytes\|NetworkOutBytes' \
  xufei/cp_mv_for_master -- \
  pkg/server pkg/session pkg/executor pkg/util/topsql pkg/mvservice docs/note/materialized_view
```

## 当前工作日志

| 日期 | 动作 | 结果 |
| --- | --- | --- |
| 2026-07-22 | 初步审计 source diff 中非 MV candidate | 形成三类：倾向排除、MV prerequisite candidate、混合文件需 hunk 级审计 |
