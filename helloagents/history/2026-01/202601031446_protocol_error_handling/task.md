# 任务清单（Lightweight Iteration）

时间戳：2026-01-03 14:46
Feature：protocol_error_handling

## 目标

- 修复 RESP2 pipeline 在协议错误与 `$-1`（null bulk string）参数场景下的连接断开问题：改为返回明确的 RESP Error，并在协议错误时关闭连接。

## Tasks

- [√] 在 `YierdisFastCommandProcessor` / `CommandProcessor` 中统一拒绝非 PING/ECHO 的 null bulk string 参数，避免 NPE 触发断线
- [√] 在 Netty handler（`exceptionCaught`）中把协议错误写回 `-ERR ...` 并关闭连接
- [√] 补充测试：null key 返回错误且连接可继续；协议错误返回错误并关闭连接
- [√] 同步更新知识库：protocol/server/command 文档补充错误处理约定
- [√] 更新 `helloagents/CHANGELOG.md` 记录修复
