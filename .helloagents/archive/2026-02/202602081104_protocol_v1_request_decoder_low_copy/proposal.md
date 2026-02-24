# 变更提案: protocol_v1_request_decoder_low_copy

## 元信息
```yaml
package: 202602081104_protocol_v1_request_decoder_low_copy
timestamp: 202602081104
type: Execution Command
status: ✅ Completed
migrated_at: 2026-02-24 23:14:10
```

## 文档

- 需求与背景（why）：[why.md](./why.md)
- 方案与设计（how）：[how.md](./how.md)
- 任务清单（tasks）：[tasks.md](./tasks.md)

## 摘要
- 标题: Change Proposal: Custom Protocol v1 request 解码低拷贝化
- 涉及模块: `yierdis-protocol-netty`, `yierdis-protocol`, `yierdis-server`（pipeline 装配复用 decoder 行为）
