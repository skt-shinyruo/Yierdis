<!-- migrated_from: history/2026-02/202602061601_custom_protocol_v1/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Custom Protocol v1（完全替换 RESP）

Directory: `helloagents/plan/202602061601_custom_protocol_v1/`

---

## 1. 协议抽象层（protocol / core API）
- [√] 1.1 设计并新增协议无关接口：`Command`/`ReplyWriter`/`Session`（最小集合），放入 `yierdis-protocol/src/main/java/...`，用于替代 `RespCommand/RespWriter` 作为 core 的 API，verify why.md#requirement-现有功能在新协议下可用命令语义保持-Scenario-常用命令
- [√] 1.2 让 `yierdis-protocol` 现有 RESP 实现以“legacy”方式实现上述接口（桥接期），并为新协议实现预留扩展点，depends on task 1.1
- [√] 1.3 将 `yierdis-core` 命令处理器入口（例如 command processor/registry）改为依赖协议无关接口（先入口、后各命令文件），depends on task 1.2

## 2. 自定义协议：JSON codec 与 framing（protocol）
- [√] 2.1 实现最小 JSON writer（保证单行输出、控制字符转义、限长）与最小 JSON parser（仅支持 object/array/string/number/bool/null），放入 `yierdis-protocol/src/main/java/...`，verify why.md#requirement-自定义协议切帧与错误可恢复resync-Scenario-正常-requestreply单条
- [√] 2.2 实现 request framing 解析与 resync 策略（`<len>:<json>\\n`），并提供可复用的“丢弃到下一帧边界”工具，depends on task 2.1
- [√] 2.3 为 JSON codec/framing 增加单元测试覆盖（长度、截断、非法 JSON、注入字符等），depends on task 2.2

## 3. Netty codec（protocol-netty）
- [√] 3.1 新增 server 侧 `CustomRequestDecoder`（ByteToMessageDecoder）：输出 `CustomCommand`（实现 Command 接口），协议错误不抛出致命异常而是产出可写回的错误事件，verify why.md#requirement-自定义协议切帧与错误可恢复resync-Scenario-非法帧不支持帧
- [√] 3.2 新增 server 侧 `JsonLineReplyEncoder`：把 `ReplyValue/ReplyWriter` 写出的结果编码为一行 JSON + `\\n`，depends on task 2.1
- [√] 3.3 新增 client 侧 reply decoder（Line-based）：按 `\\n` 切分 JSON reply，并做基本限长防御，depends on task 2.1

## 4. server 切换与错误模型（server）
- [√] 4.1 替换 `YierdisServerChannelInitializer` pipeline：移除 `RespCommandDecoder`，接入自定义 decoder/encoder；确保协议错误不再默认断连，depends on task 3.1
- [√] 4.2 调整 `YierdisFastCommandHandler/NettyCommandExecutor`：区分“协议错误可恢复”与“内部错误”，协议错误返回 `ok=false` 并继续，内部错误按现有策略处理（可选断连），depends on task 4.1
- [√] 4.3 为 QUIT/close-after-reply 语义在新协议下保留，并补齐集成测试，depends on task 4.2

## 5. client/CLI 切换（client）
- [√] 5.1 改造 `YierdisClient.execute(...)`：发送自定义 request framing + JSON payload（UTF-8），读取 JSON reply line 并以 `String/JsonValue` 形式返回，verify why.md#requirement-clientclibench-同步迁移-Scenario-CLI-单次执行与-REPL，depends on task 3.3
- [√] 5.2 改造 `YierdisCli`：打印 JSON reply（保持单行），并更新 REPL 输入解析（参数仍用当前解析规则即可），depends on task 5.1
- [√] 5.3 更新 client 侧测试（移除/替换 RESP3 push 相关断言，改为新协议的 error/keep-alive/pipelining 覆盖），depends on task 5.2

## 6. bench 切换（bench）
- [√] 6.1 改造 bench request writer：改为自定义 framing + JSON request（PING/GET/SET），depends on task 2.2
- [√] 6.2 改造 bench reply reader：改为按行读 JSON，并更新 strictReplies 校验逻辑，depends on task 6.1

## 7. Security Check
- [√] 7.1 执行安全检查（输入校验、长度上限、JSON 转义、错误回显限长、DoS 风险），并修复发现的问题（不引入明文敏感信息）

## 8. Documentation Update（Knowledge Base + README）
- [√] 8.1 更新 `README.md`：移除 RESP2/RESP3/redis-cli 的使用说明，替换为自定义协议说明与 CLI/bench 示例
- [√] 8.2 更新 `helloagents/wiki/overview.md` 与 `helloagents/wiki/modules/*`：协议边界、踩坑、Roadmap 口径同步

## 9. Testing
- [√] 9.1 增加 server 集成测试：非法 request 不断连 + 正常 request 仍可执行（覆盖 resync）
- [√] 9.2 回归运行 `mvn test`（全模块），确保无残留 RESP 依赖导致的测试失败
