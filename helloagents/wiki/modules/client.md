# client

## Purpose

提供内置的极简 RESP2 CLI 客户端，便于本地调试与脚本化测试。

## Module Overview

- **Responsibility:** 连接管理、命令输入、RESP2 编解码（客户端侧）、输出显示（支持 hex）
- **Status:** ✅Stable
- **Last Updated:** 2026-01-14

## Specifications

### Requirement: 便捷调试
**Module:** client
提供单次执行与交互两种模式，默认连接 `127.0.0.1:6378`。

#### Scenario: 二进制输出观察
条件：用户使用 `--hex`
- 预期：bulk string 用 hex 输出，便于观察二进制数据结构（例如 bitmap/hll）

## Dependencies

- `yierdis-protocol`（客户端侧复用 RESP 对象模型/codec）
- Netty（连接管理与 IO）

## Change History

- （暂无）
