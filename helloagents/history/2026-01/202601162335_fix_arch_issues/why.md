# 变更提案：修复 5 个架构/实现问题（server/client/bytes 交界处）

## 需求背景

当前代码在 server/client 的边界处存在若干“行为正确性 + 依赖边界 + 可维护性”的问题，集中体现在：

1. **QUIT 的执行路径绕过执行器，破坏顺序语义**：`yierdis-server` 在 I/O handler 里直接处理 `QUIT` 并立刻关闭连接，可能导致 pipeline 场景下“先前命令的响应被丢弃/后续命令仍被执行产生副作用”等顺序不一致问题（见 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`）。
2. **ByteBuf→BytesSink 适配器放在 offheap-netty 中且使用 Deprecated 兼容别名**：server 的 RESP 写回路径依赖 `yierdis-offheap-netty` 的 `YierdisNettyByteBufSink`（并间接依赖 deprecated 的 `YierdisBytesSink/YierdisDirectBytesSink`），使模块边界变得不直观，也与知识库中 “server 仅依赖 protocol-netty/core/args” 的描述存在偏差。
3. **client 超时后连接可能进入“失序”状态**：`yierdis-client` 在等待响应超时后仍保持同一连接继续执行后续命令，Redis 协议是严格按顺序配对的，这会在“超时但服务端稍后返回响应”的情况下产生响应错配风险（见 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`）。
4. **CLI 帮助信息硬编码 jar 名/版本**：`yierdis-client` 的 `--help` 输出包含硬编码版本号，容易与 Maven 构建版本漂移，不符合项目其它部分的“version SSOT（资源注入）”策略（见 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`）。
5. **server main 对参数错误返回码不明确**：启动参数非法时直接 `return`，对脚本/CI/运维场景不友好（见 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`）。

## 变更内容

1. 将 `QUIT` 纳入统一命令执行路径（走 `NettyCommandExecutor`），保证与其它命令同序执行，并在回复后按“close-after-reply”策略关闭连接。
2. 引入一个明确的 “Netty bytes adapter” 边界（避免 server 直接引用 offheap-netty 的 sink），逐步替换 deprecated 兼容别名，保持 off-heap slice 写出仍可走零拷贝/少拷贝 fast-path。
3. client 超时视为连接失序：超时后主动关闭连接并阻止复用，避免响应错配；必要时提供显式的重新连接策略（由调用方创建新 client）。
4. client/CLI 也采用 version SSOT（资源注入）：在 `--help`/usage 输出中使用运行时读取的 `project.version`（或等价资源），避免硬编码漂移。
5. server main 对非法参数返回非 0 退出码（或等价行为），提升脚本化可观测性与失败可诊断性。

## 影响范围

- **Modules**
  - `yierdis-server`：命令入口、执行器与连接生命周期
  - `yierdis-core`：新增/调整 server 级命令（QUIT）与连接控制抽象（Netty-free）
  - `yierdis-protocol` / `yierdis-bytes`：bytes sink 抽象的使用方式（可能涉及兼容别名收敛）
  - `yierdis-offheap-netty`：off-heap slice 写出 fast-path 适配
  - `yierdis-client`：超时与 help/version 输出
- **Files（预估）**
  - server：`YierdisFastCommandHandler`、`NettyCommandExecutor`、`YierdisServer`
  - core：`ServerCommands`（新增 QUIT 或抽象下沉）
  - bytes/offheap：ByteBuf sink 位置与接口适配
  - client：`YierdisClient`、`YierdisCli`、`pom.xml`（资源注入）

## 核心场景

### Requirement 1：pipeline 顺序语义下的 QUIT
**Module:** server

#### Scenario：`PING; QUIT` pipeline
条件：同一连接 pipeline 发送 `PING` 再发送 `QUIT`
- 预期：先收到 `PONG`，再收到 `OK`，随后连接关闭
- 预期：不会出现 “先写 OK 立即关闭导致 PONG 被丢弃” 的情况

#### Scenario：`SET a 1; QUIT; INCR a` pipeline
条件：同一连接中 `QUIT` 后仍存在已读入的后续命令
- 预期：`QUIT` 后连接关闭，`INCR` 不应产生副作用（不执行或不提交到 DB）

### Requirement 2：bytes/netty 适配边界清晰
**Module:** bytes/offheap/server

#### Scenario：off-heap string 的 GET 回包写出
- 预期：off-heap slice 写出仍可识别为 “Netty ByteBuf sink” 并走 fast-path（避免退化为频繁 heap copy）

### Requirement 3：client 超时不导致响应错配
**Module:** client

#### Scenario：超时后继续 execute
条件：执行 A 超时，但服务端稍后返回 A 的响应；随后执行 B
- 预期：client 不复用连接（关闭/标记不可用），避免将 A 的响应误当作 B 的响应

### Requirement 4：CLI help/version 不硬编码
**Module:** client

#### Scenario：版本升级后 help 输出
- 预期：`--help` 中展示的版本与构建版本一致（来自资源注入/运行时读取）

### Requirement 5：server 参数错误可脚本化识别
**Module:** server

#### Scenario：非法参数
- 预期：返回非 0 退出码（或明确错误信号），并保持现有 usage 输出

## 风险评估

- **风险：连接关闭语义变化**：QUIT 由 I/O handler 迁移至执行器后，行为会更接近 Redis（正确性提升），但需要确保“QUIT 后不再执行后续命令”的规则明确且有测试覆盖。
- **风险：模块依赖调整**：bytes/netty adapter 抽离会牵涉多个模块的依赖与包名迁移，需要避免循环依赖与 API 泄漏。
- **风险：性能回退**：off-heap slice 写出 fast-path 若识别逻辑不一致，可能退化为 copy；需在实现后通过 bench 或测试验证。
- **风险：client 行为变更**：timeout 后关闭连接属于行为变化，需要在 CLI 输出中给出可理解的错误信息，并保持最小惊讶原则。

