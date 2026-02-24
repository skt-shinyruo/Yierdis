# 任务清单（Lightweight Iteration）

Feature: inline_parser
Timestamp: 202601080927

## Tasks

- [√] 扩展 inline command 解析：支持单/双引号、反斜杠转义、`\\xHH` 十六进制转义（对齐 `sdssplitargs` 风格）
- [√] 补充单元测试覆盖：引号/转义/hex 与非法输入的协议错误回复
- [√] 同步更新知识库与 README：inline command 能力边界与调试建议
- [√] 验证：运行 `mvn -q test`
- [√] 迁移方案包到 `helloagents/history/` 并更新 `helloagents/history/index.md`
