# 2026-04-18: Busy-Only Flow Control (Simplify Backpressure)

## Summary

本项目目前的 backpressure 实现包含多处 `autoRead` 开关与连接级/全局水位线控制。由于项目仍处于开发早期，决定将行为简化为 **busy-only**：

- 过载时只返回 `ERR busy <reason>`（fail-fast），不再通过 Netty `Channel.config().setAutoRead(false/true)` 做任何“停读/恢复读”。
- 始终保持“继续读入并解析请求”的行为；拒绝发生在 executor submit 阶段。
- 仍然保留全局执行队列的**有界预算**（task count + queued bytes），防止内存不可控增长。
- 移除所有 backpressure CLI 参数与对外可观测字段，同时预留一个很薄的 flow-control SPI，方便未来重新引入背压策略而不重构 submit/drain 主链路。

## Goals

- 删除当前 backpressure（`autoRead` 翻转、连接级水位线、write-buffer backpressure handler）的实现与配置入口。
- 保留 `ExecutorBacklogBudget` 的 “slot + bytes” 预算与 `ERR busy <reason>` 诊断能力。
- 提供一个默认 no-op 的 SPI 位点，未来可扩展为：
  - 连接级背压（`autoRead` hysteresis）
  - slow client 防护（outbound buffer / write buffer）
  - 更细粒度的 reject/limit 策略

## Non-Goals

- 本次不实现任何背压、限速、断连或 write-buffer 防护策略。
- 本次不尝试解决 busy storm（客户端无限重试）问题，仅维持现有 fail-fast 语义。
- 本次不改变协议 decoder 的输入上限语义（`--protocolMaxBulkBytes/--protocolMaxArgs/--protocolMaxLineBytes`）。

## Current State (Before)

- 连接级背压：
  - `pending >= backpressureHighWatermark` 或 `pendingBytes >= backpressureBytesHighWatermark` 时禁用 `autoRead`
  - `pending <= backpressureLowWatermark` 且 bytes/global 条件满足时再启用 `autoRead`
- 全局背压：`ExecutorBacklogBudget.isGlobalBackpressureHigh/Cleared()` 触发更多 `autoRead` disable/enable 与全局恢复扫描。
- pipeline 中还有 `WriteBufferBackpressureHandler`：连接不可写时禁用 `autoRead`，可写后请求 executor 重新评估并恢复读。

## Proposed Design (After)

### 1) Remove Backpressure CLI / Runtime Config

彻底移除以下 CLI flags 及其在 runtime config 中的字段与校验：

- `--backpressureHigh`
- `--backpressureLow`
- `--backpressureBytesHigh`
- `--backpressureBytesLow`

相应地从 `INFO YIERDIS` / `STATS` 等结构化信息中移除背压相关键值（watermark、enter/exit、autoReadDisabled 等）。

### 2) Remove Netty autoRead Flipping (Always-Read)

全链路不再调用/依赖：

- `Channel.config().setAutoRead(false/true)`
- 连接可写性回调驱动的 `autoRead` 重新评估
- “closing 之后 stop reading” 的行为

所有拒绝/过载反馈统一走 `ERR busy <reason>`。

注意：连接 close-after-reply / internal error 仍会关闭连接，但不会额外通过 `autoRead=false` 来减少读取。

### 3) Keep Bounded Executor Budget + Busy Reasons

保留并继续作为过载 SSOT：

- `--executorQueueCapacity`：全局 task count 硬上限
- `--executorQueueMaxBytes`：全局 queued bytes 上限（`0` 表示禁用）

当无法 submit 时，保持现有 `ERR busy <reason>` 可诊断语义：

- `not_running`
- `queue_full`
- `bytes_budget`
- `offer_failed`

### 4) Reserve Flow-Control SPI (Default No-Op)

新增一个 server 内部 SPI（默认 no-op），在两处稳定点调用：

- submit 被拒绝时（产生 `ERR busy` 之前/之后均可，按实现选择）
- 命令执行完成并释放预算后（`onCommandFinished`）

接口草案（可按实现微调，但必须满足“低侵入 + 可扩展”）：

```java
package yier.bubu.redis;

import io.netty.channel.Channel;

interface NettyFlowControl {
    default void onSubmitRejected(Channel ch, NettyCommandExecutor.SubmitRejectReason reason) {}
    default void onCommandFinished(Channel ch) {}
}
```

默认实现 `NoopNettyFlowControl` 什么也不做。未来版本可提供替代实现来重新启用 `autoRead` 背压或其它策略，而无需重构 submit/drain 主路径。

## Observability

- 保留现有 `ERR busy <reason>` + `STATS` 计数器（submit accepted / rejected by reason）。
- 移除背压相关计数器（enter/exit、autoReadDisabled）。
- `pending/pendingBytes` 作为连接级“排队深度”统计可保留（即使当前不用于背压决策），用于后续诊断与可能的策略实现。

## Risk / Trade-offs

- **慢客户端风险**：由于不再在 `isWritable=false` 时停读，慢客户端可能导致 outbound buffer 持续增长，进而带来内存风险甚至 OOM。
- **busy storm**：客户端在收到 `ERR busy` 后持续重试会导致 server 仍然承受 decode/parse 成本。本次不处理该问题。
- **closing 窗口**：internal error 或 close-after-reply 期间仍可能继续读入并解析，直到连接真正关闭；这符合“always-read”目标，但可能产生额外开销。

## Testing Plan

- 删除/改写所有依赖 `autoRead` 翻转与背压水位线的单元测试。
- 新增/保留 busy-only 回归用例：
  - queue full / bytes budget 下返回 `ERR busy <reason>`
  - submit 失败路径不会泄漏 slot/queued-bytes 预算

## Rollout Notes

- 该变更为行为变化：生产/公网环境不建议启用该模式。当前定位为开发早期的简化实现。
- 后续如需恢复背压，优先通过 `NettyFlowControl` SPI 引入可控策略，并再引入对应 CLI 参数。

