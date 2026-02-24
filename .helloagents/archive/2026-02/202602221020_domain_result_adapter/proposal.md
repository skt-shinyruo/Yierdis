# 变更提案: domain_result_adapter

## 元信息
```yaml
package: 202602221020_domain_result_adapter
timestamp: 202602221020
type: Standard Development
status: ✅ Completed
migrated_at: 2026-02-24 23:14:10
```

## 文档

- 需求与背景（why）：[why.md](./why.md)
- 方案与设计（how）：[how.md](./how.md)
- 任务清单（tasks）：[tasks.md](./tasks.md)

## 摘要
- 标题: Change Proposal: core 分层解耦（domain result → adapter，移除 db/数据结构对 ReplySink 的依赖）
- 涉及模块: - `yierdis-core`（主要改动：ops/db/command）
