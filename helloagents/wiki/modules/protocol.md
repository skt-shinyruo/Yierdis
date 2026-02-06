# protocol

## Purpose

提供 RESP 对象模型与响应写出能力（RESP2/RESP3 最小子集），并定义 Netty-free 的 `RespFrame/RespSession` 抽象，作为协议层 SSOT。

说明：Netty codec/adapters（decoder/encoder/ByteBuf 适配）位于 `yierdis-protocol-netty`。

归属：`yierdis-protocol`（`yier.bubu.redis.protocol.*`），作为 RESP 协议对象模型与写出路径 SSOT（Netty-free）。

## Module Overview

- **Responsibility:** RESP 对象模型 + RESP2/RESP3 回复写出（连接级协议状态）+ `RespFrame/RespSession` 抽象
- **Status:** ✅Stable
- **Last Updated:** 2026-02-06

## Specifications

### Requirement: Frame/Session 抽象（Netty-free SSOT）
**Module:** protocol

为了让 `yierdis-core` 与 `yierdis-protocol` 不直接依赖 Netty，同时保留零拷贝与连接级协议状态：
- `RespFrame` 负责承载命令参数的 bytes view，并提供可选的 memory address 能力（用于 off-heap fast-path）
- `RespFrame` 继承自 `BytesSource`（定义于 `yierdis-bytes`），用于复用通用 bytes 视图抽象；这不等同于协议层依赖某个具体 off-heap 后端
- `RespFrame.length()` 提供 frame 的稳定长度（逻辑长度）
- `RespFrame.retainedBytes()` 提供更接近真实驻留内存的估算（用于 server backlog/backpressure 的 bytes 预算口径）
- `RespSession` 负责承载连接级协议状态（RESP2/RESP3），供 `RespWriter` 在写出时选择对应编码
- 具体 codec 与 Netty 绑定的实现放在 `yierdis-protocol-netty`

### Requirement: 协议默认安全上限 SSOT（`RespLimits`）
**Module:** protocol

为避免 server/client/codec/parser 在默认值上发生漂移（导致 DoS 风险、线上行为不一致或“参数看似可调但默认不一致”）：
- `RespLimits` 作为 RESP 相关默认上限的 **Single Source of Truth**
- 典型覆盖：`maxBulkBytes/maxArgs/maxLineBytes` 以及 reply 解析相关的 `maxArrayLen/maxNestingDepth`
- 期望：`yierdis-args`（server 参数默认值）、`RespObjectParser`、Netty decoder 默认值保持一致，并通过单测锁定

### Requirement: RESP wire skip/scan SSOT（`RespWireSkipper`）
**Module:** protocol

RESP 协议解码存在 request fast-path、reply framing、CLI/调试 parser 等多层消费形态。为避免多处实现 scan/skip 语义导致漂移：
- `RespWireSkipper` 提供 **Netty-free** 的 wire-level skip 能力：在不构建对象树的前提下定位一个完整 RESP 值的边界（endIndex / -1 表示数据不足）
- 支持 RESP3 streamed/attributes/aggregate 等形态的“边界识别”，供：
  - `yierdis-protocol-netty` 的 `RespDecoder` 做 reply 切帧（wire-preserving）
  - `yierdis-protocol-netty` 的 `RespCommandDecoder` 跳过 request 前置的 attributes metadata（`trySkipAttributeMapOnly`）
- strictness 约定：
  - reply/framing 场景对 `!`/`=` 采用宽松 bulk 语义（允许 `-1`），以保持历史兼容与 framing 宽容度
  - request/strict 场景对 `!`/`=` 禁止 `-1`，避免 attributes 等内部结构出现“空值语义不确定”导致解析漂移

### Requirement: RESP3（最小子集）握手与 nil
**Module:** protocol
在客户端通过 `HELLO 3` 协商后，服务端切换为 RESP3 回复，确保常见客户端探测路径可用。

#### Scenario: `HELLO 3` 返回 map
条件：客户端执行 `HELLO 3`
- 预期：服务端回复使用 RESP3 map（`%...`），并在连接级别记录协议版本为 RESP3

#### Scenario: RESP3 下的 nil 返回
条件：连接已处于 RESP3，且命令返回 nil（例如 `GET missing`）
- 预期：服务端返回 RESP3 null（`_`），而不是 RESP2 的 `$-1`

### Requirement: RESP3 map/set 类型（最小建模 + 解析 + 写出）
**Module:** protocol
为支持 RESP3 友好的集合类返回，协议层补齐最小类型建模与写出能力：
- `RespMap`：RESP3 map（`%`）
- `RespSet`：RESP3 set（`~`）
- `RespWriter`：支持 `mapHeader(...)` 与 `setHeader(...)`（RESP3-only）
- `RespObjectParser`：支持解析 `%` 与 `~`（用于测试/CLI/调试）

#### Scenario: set reply 形态稳定
条件：连接处于 RESP3，命令返回 set
- 预期：按 `~<count>\\r\\n` + `<count>` 个元素的顺序输出（元素本身仍复用 bulk string 等基础类型）

### Requirement: RESP3 扩展回复类型（互操作必需最小集合）
**Module:** protocol
为提升 RESP3 客户端互操作性，并为后续 push/订阅等能力铺路，补齐 RESP3 常见类型的最小写出/解析支持：
- `RespWriter`：新增/补齐 `booleanValue`（`#t/#f`）、`doubleValue`（`,`）、`bigNumberAscii`（`(`）、`verbatimString`（`=`）、`blobError`（`!`）、`attributeHeader`（`|`）、`pushHeader`（`>`）
- `RespObjectParser`：同步支持上述类型的解析，用于测试断言与 CLI/调试

说明：request 侧仍以 RESP2 array-of-bulk-strings 为主路径；RESP3 扩展主要体现在 reply 侧类型覆盖与互操作一致性。

一致性约束（避免“协议/对象层漂移”）：
- `RespWriter` 作为 reply 写出语义 SSOT
- Netty adapter `RespEncoder` 必须覆盖 `RespObject` 已建模的 RESP3 类型，并通过 round-trip 测试锁定（避免未来 pipeline 直接使用 encoder 时出现“编码不符合预期”的一致性坑）

### Requirement: RESP3 streamed strings / streamed aggregates 解析（全覆盖）
**Module:** protocol

为提升对 RESP3 规范与生态代理（proxy）/客户端的互操作性，协议层补齐 streamed 类型的对象树解析能力（用于 CLI/测试/调试断言）：
- streamed blob string：`$? ... ;0`（chunk 由 `;len` 声明），解析为等价的 `RespBulkString`（按累计长度受 `maxBulkBytes` 限制）
- streamed aggregates：`*?/%?/~? ... .`（以 `.` END），解析为等价的 `RespArray/RespMap/RespSet`
  - map 保持严格：元素必须成对（key/value），遇到 end marker 时若缺 value → protocol error
  - streamed 与非 streamed 可以嵌套，但受 `maxNestingDepth` 限制，避免恶意深度导致 DoS

说明：`RespWriter` 仍以固定长度的非 streamed 形式写出（因为 streaming 并非所有生态工具都会使用/期望）；streamed 支持的核心目标是“能切帧、能解析、能严格报错”。

### Requirement: RESP error 输出安全净化
**Module:** protocol
所有 `-ERR ...` 输出必须防御 CRLF 注入导致的响应拆分（response splitting），并限制 error 文本长度，避免异常信息导致大响应/日志污染。

#### Scenario: CRLF 注入不可拆分 RESP
条件：错误消息中包含 `\\r`/`\\n`（例如异常信息或拼接文本）
- 预期：最终写出的 RESP error 不包含除结尾 CRLF 外的任意 CR/LF 字符
- 预期：error 文本长度有上限（默认 256 chars）

## Dependencies

- `yierdis-bytes`（`BytesSource/BytesSink/BytesSlice` 通用 bytes 抽象：用于 frame/写出路径的 zero-copy/低分配；不等同于依赖具体 off-heap 后端实现）

## Change History

- 2026-01-03：补充协议错误与 `$-1`（null bulk string）参数的错误处理约定。
- 2026-01-04：在 RESP error 写出层增加 CR/LF 过滤与限长，降低 response splitting 风险。
- 2026-01-07：支持 `HELLO 3` 协商切换 RESP3（最小子集），并增加最小 inline command 解码路径。
- 2026-01-08：inline command 解析增强：支持单/双引号、反斜杠转义与 `\\xHH` 十六进制转义。
- 2026-01-15：拆分 `yierdis-protocol-netty` 承载 Netty codec/adapters；`yierdis-protocol` 收敛为 Netty-free SSOT（对象模型 + `RespWriter` + `RespFrame/RespSession` 抽象）。
- 2026-01-17：新增 `RespLimits` 作为协议默认安全上限 SSOT，并将默认值在 parser/decoder/args 之间收敛。
- 2026-01-23：补齐 RESP3 set（`~`）类型建模与写出/解析（`RespSet` + `RespWriter.setHeader` + `RespObjectParser`），供集合类命令在 RESP3 下返回 map/set。
- 2026-02-01：RESP3 reply 类型覆盖扩展：补齐 boolean/double/big number/verbatim/blob error/attribute/push 等最小集合，并在 `RespWriter` 中引入连接级 session 访问与 close-after-reply 请求标志（由 server/executor 落实连接关闭）。
- 2026-02-06：补齐 RESP wire skipper 与 parser 的一致性兜底测试：strictness（`trySkipOneStrict`）、limits 边界，以及 attributes + streamed 组合的对象化解析用例。
