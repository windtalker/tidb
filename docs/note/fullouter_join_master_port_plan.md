# FULL OUTER JOIN Master Port Plan

本文档跟踪将 `FULL OUTER JOIN` 从 MV feature branch 单独 port 到 `master` 的过程。

## 目标

把 `FULL OUTER JOIN` 作为物化视图 port 的前置功能，先独立重写到当前
`master` 基线，形成一个可以单独 review 和验证的 PR。

## Source

- 原始 PR: https://github.com/pingcap/tidb/pull/68919
- merge commit: `6ba9bb6053d6c9d850ab0781d6912980491e5c6b`
- 原始 base branch: `feature/release-8.5-materialized-view`
- 关联 issue: `pingcap/tidb#18023`
- 当前 port branch: `full_outer_join_for_master`
- 当前 base: `origin/master` at `955fd6550b`

## Port 范围

只 port PR #68919 中 FULL OUTER JOIN 自身的能力：

- `FULL OUTER JOIN` parser / AST 支持。
- `tidb_enable_full_outer_join` sysvar，默认关闭。
- planner 中 `FullOuterJoin` 逻辑 join type、物理计划生成、cost、predicate 和 join reorder guard。
- executor hash join v1 的 full outer join 行为。
- full outer join 的 planner unit/casetest、executor unit test 和 integration test。
- 原 PR 中的 full outer join design note。

不带入 MV feature branch 上其它 drift：

- MV DDL / refresh / MLog 代码。
- `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 通用 aggregate prerequisite，它们已在 `origin/master` 存在。
- `_tidb_commit_ts` SQL 可引用改动。
- 大范围 integration result churn。

## Rewrite 原则

- 以原 PR #68919 为 source of truth，但按当前 `master` 的文件结构和 API 重写。
- parser 只手改 `parser.y` / AST 源文件，`parser.go` 等 generated file 通过 parser 生成命令更新。
- 新增 Go 文件或 import 变动后，按仓库规则运行 `make bazel_prepare`。
- 每个阶段尽量保持改动可编译，避免同时混入无关 refactor。

## 任务拆分

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| P0 | 定位原始 PR、建立 master port branch 和 worktree 布局 | 已完成 |
| P1 | port docs、parser、AST 和 sysvar gate | 已完成 |
| P2 | port logical planner 语义、predicate 处理、join reorder guard | 已完成 |
| P3 | port physical planner、cost、ToPB / MPP guard | 已完成 |
| P4 | port hash join v1 executor full outer join 行为 | 已完成 |
| P5 | port / 重录 targeted tests 和 integration result | 已完成 |
| P6 | fmt、bazel metadata、targeted validation | 已完成 targeted 验证；Ready 验证有本地环境阻塞 |

## 验证计划

最小 WIP 验证：

```bash
make parser
make parser_unit_test
go test ./pkg/sessionctx/variable -run TestSysVar
go test ./pkg/planner/core/casetest/fulljoin -run TestFullOuterJoin -tags=intest -count=1
go test ./pkg/executor/test/jointest/hashjoin -run TestFullOuterJoin -tags=intest -count=1
```

PR ready 前的验证需要按仓库 Ready profile 补齐：

```bash
make bazel_prepare
make lint
```

integration test 结果文件不从 source branch 直接照搬。等 port 后按当前 master 行为重新录制
`tests/integrationtest/t/executor/jointest/full_outer_join.test`。

## 当前状态

- 已确认原始 PR 是 #68919。
- 当前主 worktree 已切到 `full_outer_join_for_master`。
- `cp_mv_for_master_base` 已移动到 `/private/tmp/tidb-cp-mv-for-master-base` worktree。
- 已完成 `master` API 适配，未沿用 feature branch 上旧的 `logicalop.JoinType`。
- 已按当前 `master` 重录 `tests/integrationtest/t/executor/jointest/full_outer_join.test`。
- `make bazel_prepare` 第一段 `bazel run //:gazelle` 已执行；第二段 Gazelle
  `update-repos` 在处理 `github.com/pingcap/tidb/pkg/parser => ./pkg/parser`
  时失败并挂住。该 replace 已存在于 `origin/master`。
- `make parser_unit_test` 中 parser 包测试执行到 coverage 汇总阶段，失败原因是本机
  Go toolchain 缺少 `go tool covdata`。已补充运行 `pkg/parser` module 下的
  `go test ./... -count=1`。
