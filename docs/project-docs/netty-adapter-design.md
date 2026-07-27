# Netty 适配边界与有界写回

本文说明 Netty 被限制在哪些模块，以及请求和回复在 Netty 对象、稳定 heap 请求、
中立 bytes contract 与有界 `ByteBuf` chunk 之间如何转换。

## 模块边界

- `yierdis-networking-netty` 负责入站 decoder、连接 handler 和 Netty pipeline 适配。
- `yierdis-networking-resp` 通过 `BytesSink` 编码 RESP，不依赖 `ByteBuf`。
- command、storage 和 native value 通过 `BytesView` / `BytesSlice` 工作，不导入 Netty。
- `yierdis-server-main` 把 reply reservation、chunk allocation、顺序写回和 channel lifecycle 接到一起。

## 入站路径

```text
ByteBuf fragments
  -> AccountedRespCumulator
  -> RespRequestDecoder
  -> retained heap argv + RequestMemoryLease
  -> ExecutionRequest
  -> executor / fast command handler
```

请求跨过 decoder 生命周期前会 materialize 成稳定 heap argv；这是 ownership 和 admission
边界，不是零拷贝路径。

## 回复路径

```text
command / storage result
  -> RedisReplyWriter
  -> RespReplyWriter
  -> ReplyReservationSink
  -> BoundedChunkedReplySink
  -> fixed-capacity ByteBuf chunks
  -> ConnectionReplySequencer
  -> Channel.write(...)
```

`RespReplyWriter` 只看到 `BytesSink`。`BoundedChunkedReplySink` 在 allocator 调用前把已预留
额度转换成 allocated credit，再创建固定上限 chunk 并登记到 `ReplySlot`。回复按 slot sequence
写回；资源和额度由唯一 cleanup owner 收敛。

## BytesSlice 输出

`RespReplyWriter.bulkString(BytesSlice)` 先写 bulk header，再同步调用 `slice.writeTo(out)`，最后
写 CRLF。native slice 当前通过可复用的 8 KiB heap scratch 分块读取，再写入有界 reply chunk。
它避免完整结果 materialization，但不承诺零拷贝。

## 生命周期和背压

- request lease 覆盖请求排队和执行生命周期。
- reply plan 在生成受控回复字节前申请容量。
- `ReplySlot` 持有 chunk、source owner 和 outbound lease，直到写回或终止清理完成。
- `BytesView` / `BytesSlice` 不得代替这些 retained owner 跨队列保存。

## 验证入口

- 入站：`RespRequestDecoderTest`, `RespIngressAdmissionTest`, `RespIngressLifecycleIntegrationTest`。
- RESP 编码：`RespReplyWriterTest`。
- 有界回复：`BoundedChunkedReplySinkTest`, `ReplyCapacityBlockedSchedulingTest`。
- 顺序与清理：`ConnectionReplySequencerTest`, `ReplyShutdownTest`, `NettyExecutionAdapterIntegrationTest`。
