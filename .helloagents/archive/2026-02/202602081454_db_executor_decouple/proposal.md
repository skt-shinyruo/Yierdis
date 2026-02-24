# 变更提案: db_executor_decouple

## 元信息
```yaml
package: 202602081454_db_executor_decouple
timestamp: 202602081454
type: Standard Development
status: ⚠️ Partial
migrated_at: 2026-02-24 23:14:10
```

## 文档

- 需求与背景（why）：[why.md](./why.md)
- 方案与设计（how）：[how.md](./how.md)
- 任务清单（tasks）：[tasks.md](./tasks.md)

## 摘要
- 标题: Change Proposal: DB/Executor 分层解耦与组件化（ReplySink + 巨型类拆分）
- 涉及模块: `yierdis-core`（db/ops/command）、`yierdis-protocol`（ReplySink 抽象）、`yierdis-server`（executor 组件化）
