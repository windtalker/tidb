# 物化视图 Master Port 跟踪文档

本文档用于跟踪从固定 source diff 到 `master` 的语义移植过程。这里的
source diff 作为完整改动清单使用，不作为可以直接应用到 `master` 的 patch
使用。

## Source 边界

```text
base: xufei/cp_mv_for_master_base
base commit: f08c648a20380ea723449c6c3eb5b171d96fd567

head: xufei/cp_mv_for_master
head commit: 6910cef840612ee85e171adb19d3e427697a65da

source range:
  xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
```

当前 source diff 规模：

```text
499 files changed, 97628 insertions(+), 25112 deletions(-)
```

后续 inventory 和 review 都以这个 range 为准：

```bash
git diff --name-status xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
git diff --stat xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
```

## Port 规则

- source diff 是最终预期行为的 source of truth。
- 不要把完整 source diff 直接当 patch 应用到 `master`；每个 slice 都要基于
  当前 `master` 的 API 和执行路径重写。
- 非 MV hunk 先保留在 inventory 里，等审计后再分类。有些非 MV 改动可能是
  MV 依赖的通用能力，有些可能已经在 `master` 上存在，有些则是不需要带入
  `master` 的 branch drift。
- 每个 slice port 前，先确认 `master` 是否已经有等价行为。如果已有，标记为
  `master 已有` 或 `仅适配`，不要重复引入。
- 每个 port 后的 slice 应尽量让目标分支保持可编译，并记录对应的最小验证命令。
- 总 tracking doc 只记录 source 边界、slice 状态和专题文档链接；hunk 级审计
  放到独立专题文档中，避免本文档膨胀成流水账。

## 专题审计文档

| 文档 | 内容 | 状态 |
| --- | --- | --- |
| `docs/note/materialized_view/mv_master_port_non_mv_audit.md` | source diff 中非 MV candidate 的分类：已确认不 cp、MV prerequisite candidate、混合文件 | 已确认第一批不 cp 决策 |

## 状态说明

| 状态 | 含义 |
| --- | --- |
| `待处理` | 还没有和 `master` 对比审计。 |
| `审计中` | 正在比较 source diff 和当前 `master`。 |
| `移植中` | 正在目标分支上重写这个 slice。 |
| `已移植` | 代码已经重写到目标分支，但最终验证可能还没完成。 |
| `已验证` | 已经移植并通过表中列出的本地验证。 |
| `master 已有` | 当前 `master` 已经有等价行为。 |
| `仅适配` | 当前 `master` 已有依赖能力，MV 代码只需要适配现有 API。 |
| `推迟` | 有意留到后续 slice 再处理。 |
| `排除` | 已审计，确认 master MV port 不需要该改动。 |
| `阻塞` | 存在 API、语义或依赖问题，解决前无法继续。 |

## Diff 区域概览

| 区域 | 文件数 | 初始角色 | 初始决策 |
| --- | ---: | --- | --- |
| `pkg/planner` | 97 | MV planner、fast refresh、plan guard，以及可能的非 MV planner drift | 按 slice 审计 |
| `tests/integrationtest` | 90 | MV SQL 覆盖，以及非 MV result drift | 按测试归属审计 |
| `pkg/executor` | 76 | MV refresh、mlog 写入、show/infoschema、delta merge agg | port |
| `pkg/ddl` | 38 | MV/MLog DDL、依赖元数据、DDL guard | port |
| `pkg/expression` | 30 | fast refresh 依赖的 aggregate 和 expression 能力 | 作为 prerequisite 审计 |
| `pkg/statistics` | 20 | MV/MLog analyze 和 stats 处理 | 核心元数据之后 port |
| `pkg/mvservice` | 20 | MV 后台服务框架 | port |
| `pkg/parser` | 19 | 语法、AST、restore、keyword、privilege | 提前 port |
| `pkg/util` | 17 | `mviewutil` 以及 TopSQL 等非 MV candidate | 拆分 direct MV 和 candidate |
| `pkg/session` | 13 | bootstrap、internal session、advisory lock | 作为 prerequisite port/审计 |
| `pkg/store` | 9 | GC、mockstore、其他 prerequisite | 审计 |
| `pkg/sessionctx` | 9 | sysvar 和 statement context 状态 | 作为 prerequisite port/审计 |
| `pkg/meta` | 9 | `TableInfo`、job args、metadata API | 提前 port |
| `docs/note` | 7 | MV 设计文档 | port docs |
| `pkg/metrics` | 5 | MV metrics 和 Grafana panel | 随 observability port |
| `br` | 5 | restore / system table 处理 | 作为非 MV candidate 或 prerequisite 审计 |
| `pkg/infoschema` | 4 | infoschema metadata table 集成 | 随 metadata/show port |
| `pkg/table` | 3 | mlog table 实现 | 随 DML capture port |
| `pkg/domain` | 3 | service、sysvar、domain wiring | 随 service port |
| `pkg/kv` | 3 | checker / option prerequisite | 审计 |
| `pkg/server` | 3 | connection / testserver 支持 | 审计 |
| `cmd` | 3 | `cmd/mirror` 改动 | 作为非 MV candidate 审计 |
| `build` | 2 | helper script | 作为非 MV candidate 审计 |
| root files | 9 | Bazel、module、owners、agent docs | 审计，避免无关 churn |
| `tests/realtikvtest` | 1 | import 路径覆盖 | 审计 |

## Port Slice

| ID | Slice | Source 路径 / 符号 | 分类 | Master 状态 | Port 动作 | 验证 | 状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| S0 | 跟踪文档 | `docs/note/materialized_view/mv_master_port_tracking.md` | 流程 | N/A | 创建 tracking doc | N/A | `已移植` | 每完成一个有意义的 port 步骤后更新本文档。 |
| S1 | 设计文档 | `docs/note/materialized_view/{mv_refresh,kill_refresh_purge,mv_log_purge,mv_compare,mv_init_build_state,mv_refresh_observability}.md` | docs | 待处理 | port 有用的设计文档，或保留为内部参考 | 文档 review | `待处理` | 文档内容要和最终 master 实现保持一致。 |
| S2 | Parser、AST 和用户语法 | `pkg/parser`、`pkg/parser/ast`、`pkg/parser/mysql/privs.go`、`pkg/parser/mview_stmt_options.go` | direct MV | 待处理 | 基于当前 parser grammar 重写语法和 AST | parser unit test 和相关 integration test | `待处理` | 包含 create/drop/alter/refresh/purge/cancel/show/compare 语法。 |
| S3 | 元数据模型和 DDL job args | `pkg/meta`、`pkg/meta/model`、`TableInfo`、MV/MLog 依赖元数据、job args | direct MV | 待处理 | port metadata field 和 job argument 编解码 | model / job args tests | `待处理` | 必须兼容 master 当前 metadata versioning 和 BR restore 语义。 |
| S4 | Bootstrap、变量和 internal session prerequisite | `pkg/session`、`pkg/sessionctx`、`pkg/domain`、`pkg/privilege` | direct MV / prerequisite | 待处理 | port MV system table、sysvar、privilege hook、internal-session 行为 | bootstrap / sysvar / session tests | `待处理` | 需要确认 master 是否已有等价 internal-session helper。 |
| S5 | MV/MLog DDL 执行 | `pkg/ddl/materialized_view.go`、`pkg/ddl/mview_schedule_expr.go`、`pkg/ddl/create_table.go`、DDL guard、notifier、schema tracker | direct MV | 待处理 | 基于 master 当前 DDL framework 重写 DDL flow | DDL executor tests | `待处理` | 包含依赖校验、schedule expression、alter/drop/truncate guard。 |
| S6 | MLog table 和 base-table DML capture | `pkg/table/tables/mview_log.go`、executor write path、`pkg/executor/internal/util/touched_rows.go` | direct MV | 待处理 | port mlog row 生成和事务写入行为 | writetest 和 integration DML tests | `待处理` | 验证 insert/update/delete、rollback、generated column、partition 行为。 |
| S7 | Manual refresh、show 和 infoschema executor | `pkg/executor/materialized_view.go`、`pkg/executor/show.go`、`pkg/executor/infoschema_reader.go` | direct MV | 待处理 | port refresh executor、show command、infoschema reader | refresh 和 infoschema tests | `待处理` | 包含 complete refresh 变体和用户可见 metadata 输出。 |
| S8 | MV service 框架 | `pkg/mvservice`、`pkg/domain`、`pkg/server`、service metrics | direct MV | 待处理 | port 后台调度、cancel、backpressure 框架 | mvservice unit tests | `待处理` | 依赖 metadata、bootstrap table 和 internal session 变量。 |
| S9 | Fast refresh planner | `pkg/planner/mview`、`pkg/planner/core`、plan guard、mview casetest | direct MV | 待处理 | 基于当前 planner 重写 fast-refresh plan derivation | planner casetest 和 unit tests | `待处理` | 包含 count/sum/min/max 和 bounded fast refresh planning。 |
| S10 | Delta merge agg executor 和 aggregate prerequisite | `pkg/executor/mviewdeltamergeagg`、`pkg/executor/aggfuncs`、`pkg/expression/aggregation` | direct MV / prerequisite | aggregate prerequisite 部分 `master 已有` | port fast refresh 需要的 MV operator；`SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 直接适配 `origin/master` 现有实现 | executor aggregate tests 和 mviewdeltamergeagg tests | `待处理` | `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 已有 parser / expression / executor / tipb pushdown / checker 支持。 |
| S11 | Observability、metrics、stats 和 GC 处理 | `pkg/metrics`、`pkg/statistics`、`pkg/store/gcworker`、refresh observability | direct MV / hardening | 待处理 | port metrics、history、analyze skip/schedule、GC safeguard | targeted metrics / stats / gc tests | `待处理` | 通常放在 core refresh 和 service 之后更稳。 |
| S12 | BR / import / restore / system-table 交互 | `br`、`pkg/executor/import_into.go`、importer tests、realtikv import test | prerequisite / candidate | 待处理 | 审计 MV system table 和 initial build 是否依赖这些改动 | 如果 port，则跑 targeted BR/import tests | `待处理` | 部分改动可能 master 已有，或者和 MV 无关。 |
| S13 | 非 MV candidate drift | `cmd/mirror`、`full_outer_join`、`active_active/commit_ts`、TopSQL、build helpers、root metadata | non-MV candidate | 部分已确认 | agent docs、`.gitignore`、build helper、`cmd/mirror`、`google/skylark` 删除、root metadata、TopSQL network bytes、prepare dedup / plan cache、active-active commit TS 独立测试均不 cp；FULL OUTER JOIN 转入 prerequisite 审计；`_tidb_commit_ts` 底层能力 `origin/master` 已有，但 SQL 可引用策略仍需在 MV port 中处理；`SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 已确认 `master` 已有 | 只有 port 时才跑 targeted test | `审计中` | integration result 不直接 cp，后续按 master port 后实际行为重新录制。 |
| S14 | Bazel 和生成文件元数据 | `BUILD.bazel`、`DEPS.bzl`、`go.mod`、`go.sum`、generated parser output | build metadata | 待处理 | 源码改完后基于 master 重新生成 | `make bazel_prepare`；需要时跑 parser 生成命令 | `待处理` | 需要生成时不要手改 generated artifact。 |
| S15 | Integration 和 regression tests | `tests/integrationtest`、executor/DDL/planner tests | tests | 待处理 | 按 slice port 测试和行为 | scoped integration / unit commands | `待处理` | 避免从无关 planner 改动带来大量 result churn。 |

## 非 MV Candidate Inventory

下面这些路径出现在 source diff 中，但不是明显的 MV 专属改动。它们需要保留在
清单里，直到完成分类。详细审计见：

```text
docs/note/materialized_view/mv_master_port_non_mv_audit.md
```

| Candidate | Source 路径 | 初始疑点 | 决策 |
| --- | --- | --- | --- |
| Full outer join | `pkg/planner/core/casetest/fulljoin`、join executor 改动、`tests/integrationtest/*full_outer_join*` | 可能和 MV 无关，但 planner/executor 改动可能影响 MV query 支持 | 作为 MV prerequisite 审计 |
| Active-active commit TS | `tests/integrationtest/*active_active/commit_ts*` | 大概率是非 MV integration coverage | 独立测试不 cp；`origin/master` 已有底层 `_tidb_commit_ts` 能力，但直接 SQL 引用仍被禁用，MV 内部 SQL 的使用方式后续单独处理 |
| TopSQL 改动 | `pkg/util/topsql` | 大概率是非 MV drift，除非 refresh observability 依赖它 | 不 cp |
| BR restore 改动 | `br/pkg/restore/snap_client` | 可能是 MV system table 和 metadata restore 所需 | 待处理 |
| `cmd/mirror` | `cmd/mirror` | 大概率是无关工具 drift | 不 cp |
| Build helper | `build/detect_base_branch.sh`、`build/get_changed_bazel_pkgs.sh` | 大概率是 workflow drift | 不 cp |
| Root metadata | `AGENTS.md`、`DEPS.bzl`、`Makefile`、`OWNERS`、`OWNERS_ALIASES`、`go.mod`、`go.sum`、`.gitignore` | 避免带入无关仓库级 drift；如果确实需要依赖，应该从 master 重新生成 | agent docs、`.gitignore`、OWNERS、Makefile 等不 cp；必要 build metadata 后续从 master 重新生成 |

## 工作日志

| 日期 | 目标分支 | Slice | 动作 | 结果 |
| --- | --- | --- | --- | --- |
| 2026-07-22 | `cp_mv_for_master_base` | S0 | 创建 source-boundary tracking 文档 | `已移植` |
| 2026-07-22 | `cp_mv_for_master_base` | S13 | 记录第一批非 MV drift 人工确认决策 | agent docs、`.gitignore`、build helper、`cmd/mirror`、`google/skylark` 删除、root metadata、TopSQL network bytes、prepare dedup / plan cache、active-active commit TS 独立测试均不 cp；integration result 后续重新录制 |
| 2026-07-22 | `cp_mv_for_master_base` | S13 | 确认最新 `origin/master` 的 `_tidb_commit_ts` 状态 | 底层 commitTS 下传和隐藏列建模已有；preprocess 仍禁止直接 SQL 引用，MV port 需要决定是完全放开还是限定 internal/MLog 使用 |
| 2026-07-22 | `cp_mv_for_master_base` | S10/S13 | 确认最新 `origin/master` 的新 aggregate 状态 | `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 已有 parser / expression / executor / tipb pushdown / checker 支持；fast refresh 后续只适配 master 现有实现 |

## 常用命令

查看固定 source range：

```bash
git diff --name-status xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
git diff --stat xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
```

查看某一组路径：

```bash
git diff xufei/cp_mv_for_master_base..xufei/cp_mv_for_master -- pkg/parser pkg/parser/ast
git diff xufei/cp_mv_for_master_base..xufei/cp_mv_for_master -- pkg/ddl
git diff xufei/cp_mv_for_master_base..xufei/cp_mv_for_master -- pkg/executor pkg/table
git diff xufei/cp_mv_for_master_base..xufei/cp_mv_for_master -- pkg/planner pkg/mvservice
```

检查 `master` 是否已有某个符号或行为：

```bash
git grep -n "<symbol-or-keyword>" master -- <path-or-dir>
```

按区域统计 source diff：

```bash
git diff --name-only xufei/cp_mv_for_master_base..xufei/cp_mv_for_master \
  | awk -F/ '{ if ($1 == "pkg") print $1 "/" $2; else if ($1 == "tests") print $1 "/" $2; else if ($1 == "docs") print $1 "/" $2; else print $1 }' \
  | sort | uniq -c | sort -nr
```

## 待确认问题

- 实际 port 到 `master` 的 target branch 名称是什么？
- 这个 port 最终拆成多个 PR，还是先在一个 target branch 里保留所有 slice，直到
  初步可编译后再拆？
- 哪些非 MV candidate 是有意包含在 master port source boundary 里的，哪些只是
  incidental branch drift？
