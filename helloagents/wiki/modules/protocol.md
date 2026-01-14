# protocol

## Purpose

实现 RESP2/RESP3（最小子集）的命令解码与响应写出，并提供 inline command 解析（调试用，兼容 `sdssplitargs` 风格）。

## Module Overview

- **Responsibility:** RESP2 framing（命令请求）+ inline command（调试）+ RESP2/RESP3 回复写出（连接级协议状态）
- **Status:** ✅Stable
- **Last Updated:** 2026-01-08

## Specifications

### Requirement: 命令解码（RESP2 multi-bulk + inline）
**Module:** protocol
将 TCP 字节流解码为命令对象（含 argc 与每个参数的 bytes view），并在不改变 fast-path 的前提下增加 inline command 支持（调试用）。

#### Scenario: 二进制安全参数
条件：客户端发送包含任意字节的 bulk string 参数
- 预期：服务端按原样读取 bytes，不进行 UTF-8 强制解码（仅在错误提示等需要时转换）

#### Scenario: 协议错误（非法 RESP）
条件：客户端发送非法请求（例如非 `*`/inline 的非法前缀、bulk string CRLF 不完整、行过长等）
- 预期：服务端返回 `ERR Protocol error: ...` 并关闭连接（防止半包/乱序状态继续污染后续解析）

#### Scenario: null bulk string 参数（`$-1`）
条件：客户端发送包含 `$-1` 的 bulk string 参数
- 预期：仅允许 `PING/ECHO` 的单参数消息为 `$-1`（返回 `(nil)`）；其余命令若出现 `$-1` 参数，返回 `ERR Protocol error: null bulk string`（避免触发 NPE/断线）

#### Scenario: inline command 引号与转义（`sdssplitargs` 风格）
条件：客户端使用 inline command，参数包含空格/引号/反斜杠转义/`\\xHH` 十六进制转义
- 预期：服务端解析得到正确的 argv bytes（二进制安全，不强制 UTF-8），支持常见调试输入（例如 `ECHO "hello world"\\r\\n`）
- 预期：双引号内支持反斜杠转义（`\\n`/`\\r`/`\\t`/`\\b`/`\\a`/`\\"`/`\\\\` 等）与 `\\xHH` 字节转义；单引号内仅支持 `\\'`
- 说明：inline 分支会对参数做“解码+物化”（非零拷贝），主要用于 `telnet/nc` 调试场景；生产客户端应优先使用 RESP2/RESP3 multi-bulk

### Requirement: RESP3（最小子集）握手与 nil
**Module:** protocol
在客户端通过 `HELLO 3` 协商后，服务端切换为 RESP3 回复，确保常见客户端探测路径可用。

#### Scenario: `HELLO 3` 返回 map
条件：客户端执行 `HELLO 3`
- 预期：服务端回复使用 RESP3 map（`%...`），并在连接级别记录协议版本为 RESP3

#### Scenario: RESP3 下的 nil 返回
条件：连接已处于 RESP3，且命令返回 nil（例如 `GET missing`）
- 预期：服务端返回 RESP3 null（`_`），而不是 RESP2 的 `$-1`

### Requirement: RESP error 输出安全净化
**Module:** protocol
所有 `-ERR ...` 输出必须防御 CRLF 注入导致的响应拆分（response splitting），并限制 error 文本长度，避免异常信息导致大响应/日志污染。

#### Scenario: CRLF 注入不可拆分 RESP
条件：错误消息中包含 `\\r`/`\\n`（例如异常信息或拼接文本）
- 预期：最终写出的 RESP error 不包含除结尾 CRLF 外的任意 CR/LF 字符
- 预期：error 文本长度有上限（默认 256 chars）

## Dependencies

- （无）

## Change History

- 2026-01-03：补充协议错误与 `$-1`（null bulk string）参数的错误处理约定。
- 2026-01-04：在 RESP error 写出层增加 CR/LF 过滤与限长，降低 response splitting 风险。
- 2026-01-07：支持 `HELLO 3` 协商切换 RESP3（最小子集），并增加最小 inline command 解码路径。
- 2026-01-08：inline command 解析增强：支持单/双引号、反斜杠转义与 `\\xHH` 十六进制转义。
