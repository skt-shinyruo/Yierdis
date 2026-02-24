<!-- migrated_from: history/2026-01/202601162335_fix_arch_issues/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# 技术设计：修复 5 个架构/实现问题（server/client/bytes 交界处）

## 技术方案

### 核心技术

- Java 17
- Netty 4.1（TCP + pipeline）
- Maven multi-module
- RESP2/RESP3（最小子集）协议模型与 codec

### 实现要点

1. **QUIT 走统一执行路径（保证顺序）**
   - 将 `QUIT` 作为 server 命令注册到 `yierdis-core`（与 `PING/HELLO/...` 同一层级）。
   - 为命令层提供一个 **Netty-free** 的“连接控制”能力：命令能表达 “回复后关闭连接（close-after-reply）”。
   - 执行器在每个 command 执行后检查该标志：如果触发，则在 flush 后关闭 channel，并跳过该连接后续已入队但位于 QUIT 之后的任务（避免副作用）。

2. **Bytes/Netty 适配边界收敛**
   - 当前 server 写回依赖 `yierdis-offheap-netty` 的 `YierdisNettyByteBufSink`（并使用 deprecated alias 接口），导致依赖边界不直观。
   - 目标：引入一个清晰的 **Netty bytes adapter** 边界（建议新模块 `yierdis-bytes-netty`），提供 `NettyByteBufSink`（实现 `yier.bubu.redis.bytes.DirectBytesSink`）。
   - `yierdis-server`、`yierdis-offheap-netty` 统一复用该 sink；逐步淘汰 `YierdisBytesSink/YierdisDirectBytesSink` 兼容别名在热路径的出现。

3. **client 超时后连接不可复用**
   - Redis/RESP 是严格 FIFO 的 request/response 配对；超时意味着“连接状态未知”，继续复用会造成响应错配。
   - 策略：一旦 `execute()` 超时，立即关闭 channel 并将 client 标记为不可用；后续调用直接失败（提示调用方创建新连接）。

4. **client/CLI 的 version SSOT**
   - 参照 server 侧 `yierdis-version.properties` 的资源注入方式：在 `yierdis-client` 模块中同样生成该资源，并在 `--help` 输出中读取 version，而不是硬编码 jar 名/版本。

5. **server main 的退出码与启动诊断**
   - 参数非法：保持 usage 输出，但明确返回非 0 退出码（脚本可识别）。
   - 可选：启动时打印关键配置摘要（队列/背压/协议上限/off-heap 等），便于排障。

## 架构设计

```mermaid
sequenceDiagram
  participant C as Client
  participant D as RespCommandDecoder (Netty)
  participant H as YierdisFastCommandHandler
  participant E as NettyCommandExecutor (single thread)
  participant P as YierdisFastCommandProcessor (core)
  participant W as RespWriter (protocol)

  C->>D: TCP bytes (RESP2/inline)
  D->>H: RespCommand (frame + argv slices)
  H->>E: trySubmit(ctx, cmd)
  E->>P: execute(cmd, writer)
  P->>W: write reply + (optional) close-after-reply flag
  E-->>C: flush replies (coalesced)
  Note over E,C: If close-after-reply: close channel after flush; skip post-QUIT tasks
```

## Architecture Decision ADR

### ADR-20260116-05：QUIT 的 close-after-reply 语义由协议层表达，执行器负责落实

**Context：**
- 现状：`QUIT` 在 I/O handler 直接回复并关闭连接，可能破坏 pipeline 顺序语义，并让“连接生命周期控制”散落在 Netty 层。
- 目标：让 QUIT 与其它命令同序执行，并且不引入 core→Netty 的依赖。

**Decision：**
- 在 `yierdis-protocol` 增加一个轻量机制（建议在 `RespWriter` 增加 `requestCloseAfterReply()` + `closeAfterReplyRequested()`），命令层通过 writer 表达 close-after-reply。
- `NettyCommandExecutor` 在执行命令后读取该标志：
  - 对当前命令的最后一次 `write` 使用可观察的 promise，并在其完成后关闭连接（close after flush）。
  - 将连接标记为 “closing”，并在后续 drain 中跳过该连接剩余任务（只做资源回收与 pending 计数回退）。

**Rationale：**
- 保持 core 模块 Netty-free，同时消除 handler special-case，保证顺序语义与可维护性。
- 连接关闭属于 transport 层职责，应由 server/executor 落实，而不是由 core 直接触达 Netty。

**Alternatives：**
- 方案 A：继续在 handler 处理 QUIT → 拒绝原因：顺序语义错误、难以扩展更多 connection-level 命令。
- 方案 B：在 executor 内部识别 QUIT（不进 core registry）→ 拒绝原因：命令语义仍分裂；core 的命令集合不完整。

**Impact：**
- 需要调整 `NettyCommandExecutor` 的 write/flush 逻辑以支持 “write promise + close”。
- 需要新增/更新测试：pipeline 顺序、QUIT 后任务跳过、连接关闭时资源释放。

### ADR-20260116-06：引入 yierdis-bytes-netty 模块承载 ByteBuf sink，收敛依赖边界

**Context：**
- 现状：server 的写回路径直接引用 `yierdis-offheap-netty` 的 `YierdisNettyByteBufSink`，并涉及 deprecated alias。
- 目标：让 bytes 适配器位于语义更正确的模块边界，并保持 off-heap slice 写出仍有 fast-path。

**Decision：**
- 新增模块 `yierdis-bytes-netty`：
  - 提供 `yier.bubu.redis.bytes.netty.NettyByteBufSink`（实现 `DirectBytesSink`，可 unwrap ByteBuf）。
  - `yierdis-server`/`yierdis-offheap-netty` 依赖该模块，统一复用 sink。
- `yierdis-offheap-netty` 的 slice 写出 fast-path 由判断 `instanceof NettyByteBufSink` 实现。
- 逐步将 `YierdisNettyByteBufSink` 降级为兼容 wrapper（或仅在过渡期保留），避免在主干路径继续扩散 deprecated alias。

**Rationale：**
- bytes adapter 属于 I/O 适配层，与 off-heap 后端选择解耦；抽离后依赖方向更直观，知识库与代码一致性更高。

**Impact：**
- Maven modules 与依赖关系需要调整；需要在 `helloagents/wiki/arch.md` 与 `wiki/modules/*.md` 同步更新依赖图与说明。

## 安全与性能

- **安全：**
  - 保持现有协议错误的 “ERR + close” 策略；错误信息继续做 CRLF 清洗，避免 response splitting。
  - QUIT 后跳过后续任务时，确保只回收资源、不执行 DB 写入，避免产生不可预期副作用。
- **性能：**
  - 保障 off-heap slice 写出仍走 ByteBuf fast-path（避免退化为 heap copy）。
  - QUIT 的 close-after-reply 仅在触发时使用 promise；正常命令保持 `voidPromise` 的 fast-path。

## 测试与发布

- **单元/集成测试：**
  - 新增 pipeline 场景测试：`PING; QUIT` 的响应顺序与关闭行为。
  - 新增/补充 “QUIT 后任务不执行” 测试（验证 DB 不产生副作用）。
  - 新增 client timeout 行为测试：超时后 client 不可复用（或必须重连）。
  - Maven 构建层面验证：`yierdis-server` 不再依赖 `yierdis-offheap-netty`（依赖边界护栏）。
- **发布：**
  - 计划作为 refactor/fix 合并；更新 `helloagents/CHANGELOG.md` 与模块文档。

