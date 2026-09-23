# Netty 适配边界与有界写回

Netty 只出现在少数几个模块里，请求和回复在 Netty 对象、稳定 heap 请求、中立 bytes contract 与有界 `ByteBuf` chunk 之间转换。本文说明 pipeline 里每个 handler 的装配顺序与职责、为什么这么分层，以及背压在 Netty 侧的表现。

## 模块边界

- `yierdis-server` 包含入站 decoder、连接 handler 和 Netty pipeline 适配（`protocol.resp.netty` 与 `app.server` 两个包）。
- `yierdis-networking-resp` 用 `BytesSink` 编码 RESP，不依赖 `ByteBuf`；`RespReplyWriter` 只看到 `BytesSink`。
- command、storage 和 native value 通过 `BytesView` / `BytesSlice` 工作，不导入 Netty。
- `yierdis-server` 同时组装 reply reservation、chunk allocation、顺序写回和 channel lifecycle，复用 `yierdis-networking-resp` 的编码实现。

## 入站路径

```text
ByteBuf fragments
  -> AccountedRespCumulator
  -> RespRequestDecoder
  -> retained heap argv + RequestMemoryLease
  -> RespDecodedMessage.Request(ExecutionRequest) / RespProtocolError
  -> reply admission / RegisteredRespMessage
  -> CommandExecutor / CommandDispatcher
```

请求跨过 decoder 生命周期前会 materialize 成稳定 heap argv；这条路径的定位是 ownership 和 admission 边界，而非零拷贝路径。

`RespRequestDecoder` 用单一封闭 phase 保存当前 array、bulk、inline、request credit 或 handoff 恢复所需的数据，异步 admission 消费完成后才进入下一 phase。decoder 自己的 pending phase 先于 `AccountedRespCumulator` 的 consolidation admission 恢复，因此同一连接不会同时登记两种等待。

## Pipeline 装配顺序与各 handler 职责

`YierdisServerChannelInitializer.initChannel(SocketChannel)` 是唯一的装配点。它首先调用 `childChannelRegistry.admit(ch)`，未 `ACCEPTED` 就直接返回（连接不被初始化）。随后按固定顺序 `addLast(...)`：

| 顺序 | handler 名 | 类型 | 职责 |
| --- | --- | --- | --- |
| 1 | `writeBufferBackpressure` | `WriteBufferBackpressureHandler` | 监听 `channelWritabilityChanged`：不可写时 `executor.onTransportUnwritable(connection)` 并调度慢客户端宽限关闭；恢复可写时 `executor.onTransportWritable(connection)`。 |
| 2 | `idleTimeout` | `IdleStateHandler` | 仅在 `clientIdleTimeoutMillis > 0` 时加入，只设 readerIdle。 |
| 3 | `idleTimeoutCloser` | `CloseOnReadIdleHandler` | 收到 `READER_IDLE` 时走 `NettyExecutionConnection.initiateClose(channel)`。 |
| 4 | `inboundReadCredit` | `InboundReadCreditHandler` | 入站读额度与 autoRead 的暂停/恢复（`pauseIngress`/`resumeIngress`/`pauseExecutorInput`/`resumeExecutorInput`）。 |
| 5 | `inboundByteAccounting` | `InboundByteAccountingHandler` | 把 `ByteBuf` 包装成记账后的 `AccountedInboundBuffer`，交给读额度 handler。 |
| 6 | `respRequestDecoder` | `RespRequestDecoder` | RESP array/inline 解码 + 分配前 ingress admission，产出 `RespDecodedMessage`。 |
| 7 | `executionRequestIngress` | `NettyExecutionRequestIngress` | 处理 `RegisteredRespMessage`，完成 executor admission 或协议错误回包。 |

同一连接上还装配了不在 pipeline 里的三个协作组件（`initChannel` 内创建，绑定到 `NettyExecutionConnection`）：

- `InboundConnectionMemory` + `InboundMemoryBudget`：入站内存额度，单连接硬上限是 `perConnectionHardLimit(config)`，receive buffer 容量是 `min(8 KiB, protocolMaxCommandBytes)`。
- `OutboundMemoryBudget.Connection`：通过 `outboundMemoryBudget.openConnection(replyPerConnectionCapacityBytes)` 打开。
- `ConnectionReplySequencer` + `NettyReplyDecodedMessageGate`：回复顺序器与 reply admission gate；后者通过 `executionConnection.bindReplyGate(replyGate)` 绑定，并通过 `dispatcher::replyAdmissionRequirement` 解析每个请求的 `ReplyAdmissionRequirement`。

channel 级选项：`childOption(TCP_NODELAY, true)`、`childOption(SO_KEEPALIVE, true)`；当 `clientOutputBufferLimitBytes > 0` 时把 `WriteBufferWaterMark` 设为 `low = high/2`、`high = clientOutputBufferLimitBytes`，作为 Netty 判断 `channel.isWritable()` 的阈值。

## 回复路径

```text
CommandResult / RedisReply
  -> RedisReplyRenderer
  -> RedisReplyWriter
  -> RespReplyWriter
  -> ReplyReservationSink
  -> BoundedChunkedReplySink
  -> fixed-capacity ByteBuf chunks
  -> ConnectionReplySequencer
  -> Channel.write(...)
```

`RespReplyWriter` 只看到 `BytesSink`。`BoundedChunkedReplySink` 在 allocator 调用前把已预留额度转换成 allocated credit，再创建固定上限 chunk（`replyChunkPayloadBytes`，默认 64 KiB；组件开销常量 `CHUNK_COMPONENT_OVERHEAD_BYTES = 1024`）并登记到 `ReplySlot`；回复按 slot sequence 写回。`ConnectionReplySequencer` 是连接上唯一可以调用 `Channel.write(Object)` 的出口，生产侧（executor）只能把 slot 置为 READY。

slot lifecycle 是 `REGISTERED`、`WAITING_CAPACITY`、`PRODUCING`、`READY`、`WRITING`、`CLEANING`、`TERMINATED`（`ReplySlotState`），只有进入 `CLEANING` 的路径能执行终态清理，清理完成后状态变为 `TERMINATED`。cleanup completion 会等待 in-flight chunks 与异步 resource close，再关闭 outbound lease。

命令注册通过 `CommandSyntax.replyAdmissionRequirement()` 声明后续 slot 是否可继续流水线注册（`PIPELINED` 或 `BARRIER_UNTIL_CLEANUP`）。reply gate 只执行这个策略；`EXEC` 用 `BARRIER_UNTIL_CLEANUP` 提供容量屏障，不在 transport 层重新识别命令 token。gate 在注册 slot 前先向 `OutboundMemoryBudget.Connection` 预留 `replyControlReservationBytes` 并发起单回复上限预留，预留失败则 `awaitCapacity(...)` 等待而不是丢弃。

## 为什么这样分层

- **Netty 类型不外溢**。只有 `yierdis-server` 的 `protocol.resp.netty` 与 `app.server` 包导入 `io.netty.*`。`RespRequestDecoder` 在 handoff 前把 `ByteBuf` materialize 成 heap argv，`RespReplyWriter` 只写 `BytesSink`，所以 command 与 storage 层完全看不到 `ByteBuf`，也就能被 heap 输入（CLI、benchmark、测试）复用同一套请求/回复 contract。
- **记账先于分配**。`inboundReadCredit`、`inboundByteAccounting` 排在 decoder 之前，保证任一 bulk/argv 分配都已先通过 budget 准入；decoder 内部也把 array/bulk/inline/request 各自拆成独立 admission phase。
- **typed message 再交给终端消费者**。decoder 产出封闭的 `RespDecodedMessage` 变体，reply gate 把它与 `ReplySlot` 绑成 `RegisteredRespMessage`，ingress 只处理这两个变体。这样“回复顺序”在进入 executor 之前就已固定，owner thread 侧无需关心 transport。
- **关闭语义先置位再动手**。`idleTimeoutCloser`、慢客户端关闭、ingress 异常都收敛到 `initiateClose()`：先 `markClosing()`（executor 据此跳过已排队命令、回收事务），再 `channel.close()`。顺序不能交换，否则 close 完成到 closing 置位之间的窗口内已入队命令仍可能执行。

## BytesSlice 输出

`RespReplyWriter.bulkString(BytesSlice)` 先写 bulk header（`$<len>\r\n`），再同步调用 `slice.writeTo(out)`，最后写 CRLF。native slice 当前经可复用的 8 KiB heap scratch（`NativeBytesSlice.COPY_CHUNK_BYTES = 8 * 1024`）分块读取，再写入有界 reply chunk。该路径避免完整结果 materialization，但不承诺零拷贝。

## 生命周期和背压

- request lease 覆盖请求排队和执行生命周期。
- reply plan 在生成受控回复字节前申请容量。
- streaming source owner 由 `PreparedCommand` 保留到同步 renderer 返回；这个 owner 不会转移给 writer 或 `ReplySlot`。
- `ReplySlot` 持有编码后的 chunk 和 outbound lease，直到写回或终止清理完成。
- `BytesView` / `BytesSlice` 不得代替这些 retained owner 跨队列保存。

背压在 Netty 侧分三条独立通道，任何一条触发都能暂停输入：

1. **入站内存/读额度**：`InboundReadCreditHandler` 根据 `InboundMemoryBudget` 与 `InboundConnectionMemory` 暂停或恢复读；`InboundByteAccountingHandler` 保证每个 `ByteBuf` 都被记账。`NettyExecutionRequestIngress` 在 pendingSubmissions 非空时也通过它暂停 executor 输入。
2. **channel 可写性**：`WriteBufferWaterMark` 决定 `channel.isWritable()`。不可写时 `WriteBufferBackpressureHandler` 调 `executor.onTransportUnwritable(...)`（`disableAutoRead`）；重新可写时 `onTransportWritable(...)` 回到 owner thread 重新评估 `recoverInputIfPossible(...)`。若在 `clientOutputBufferOverLimitMillis` 宽限期内一直不可写，则 `initiateClose(...)` 关闭慢客户端。
3. **回复容量**：`OutboundMemoryBudget`（全局 + 每连接）在 `tryReserve` 时可能返回 `WAITING`，executor `markInputPausedByReply()` 并暂停输入，容量释放后经 `task.reply.onCapacityAvailable(...)` 回到 owner thread 继续。`REQUEST_TOO_LARGE` / `TOO_LARGE` 则按终止语义处理，不伪造替身回复。

这三条通道互相独立：入站额度不够不会影响 reply 写回，channel 不可写也不会让 reply capacity 记错账。

## 验证入口

- 入站：`RespRequestDecoderTest`, `RespIngressAdmissionTest`, `RespIngressLifecycleIntegrationTest`。
- RESP 编码：`RespReplyWriterTest`（在 `yierdis-networking-resp/src/test/java`）。
- 有界回复：`BoundedChunkedReplySinkTest`, `ReplyCapacityBlockedSchedulingTest`。
- 顺序与清理：`ConnectionReplySequencerTest`, `ReplyShutdownTest`, `NettyExecutionAdapterIntegrationTest`。