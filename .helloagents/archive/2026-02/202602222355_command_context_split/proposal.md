# 变更提案: command_context_split

## 元信息
```yaml
package: 202602222355_command_context_split
timestamp: 202602222355
type: Standard Development
status: ✅ Completed
migrated_at: 2026-02-24 23:14:10
```

## 文档

- 需求与背景（why）：[why.md](./why.md)
- 方案与设计（how）：[how.md](./how.md)
- 任务清单（tasks）：[tasks.md](./tasks.md)

## 摘要
- 标题: Change Proposal: CommandContext 全链路重构（拆分输入 Session 与输出 ReplyWriter）
- 涉及模块: - `yierdis-protocol-model`（新增 `CommandContext`，调整 `ReplyWriter/ReplyWriterFactory` 与 provider 接口）
