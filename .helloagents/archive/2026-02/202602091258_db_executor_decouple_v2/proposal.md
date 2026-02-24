# 变更提案: db_executor_decouple_v2

## 元信息
```yaml
package: 202602091258_db_executor_decouple_v2
timestamp: 202602091258
type: Execution Command
status: ✅ Completed
migrated_at: 2026-02-24 23:14:10
```

## 文档

- 需求与背景（why）：[why.md](./why.md)
- 方案与设计（how）：[how.md](./how.md)
- 任务清单（tasks）：[tasks.md](./tasks.md)

## 摘要
- 标题: Change Proposal: DB/Executor 边界解耦 v2（移除 server→YierdisDb 直接依赖 + 持续拆分巨型类）
- 涉及模块: - `yierdis-server`（executor/bootstrap/info provider 解耦与组件化）
