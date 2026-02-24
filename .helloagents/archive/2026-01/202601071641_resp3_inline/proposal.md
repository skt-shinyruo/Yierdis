# 变更提案: resp3_inline

## 元信息
```yaml
package: 202601071641_resp3_inline
timestamp: 202601071641
type: Standard Development
status: ✅ Completed
migrated_at: 2026-02-24 23:14:10
```

## 文档

- 需求与背景（why）：[why.md](./why.md)
- 方案与设计（how）：[how.md](./how.md)
- 任务清单（tasks）：[tasks.md](./tasks.md)

## 摘要
- 标题: Change Proposal: 支持 RESP3 + Inline（提升 Redis 客户端兼容性）
- 涉及模块: - `yierdis-server`（Netty pipeline、协议解析、命令处理、响应写出）
