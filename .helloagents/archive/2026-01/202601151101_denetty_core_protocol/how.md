<!-- migrated_from: history/2026-01/202601151101_denetty_core_protocol/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: core/protocol 去 Netty 依赖（边界收敛）

## Technical Solution

### Core Technologies
- Java 17
- Maven multi-module
- Netty（仅保留在 I/O 适配层与 codec 层）
- offheap-api（`YierdisBytesSource` / `YierdisBytesSink` / slice abstractions）

### Implementation Key Points
1. **分层拆分：**
   - `yierdis-protocol`：RESP 对象模型 + 命令抽象 + 写出逻辑（Netty-free）
   - `yierdis-protocol-netty`：Netty codec（decoder/encoder）+ 与 Channel/ByteBuf 的适配（Netty-only）
2. **协议状态抽象：**
   - 把“连接级 RESP2/RESP3 状态”从 `Channel` Attribute 中抽离为接口（由 server 侧实现并存储在 Channel）
3. **command/core 与协议写出解耦：**
   - `RespWriter` 改为面向 bytes sink 写入（Netty-free）
   - server 层用 `offheap-netty` 的 ByteBuf sink 适配写出
4. **Unsafe/off-heap 内存访问封装：**
   - core 内部不再直接调用 Netty `PlatformDependent`
   - raw memory 的 get/put/copy 通过 unsafe allocator 的封装能力提供（封装留在 offheap-unsafe 模块）

## Architecture Design

```mermaid
flowchart LR
  subgraph Core[SSOT / Netty-free]
    Protocol[yierdis-protocol\n(RespObject/RespWriter/RespCommand core)]
    CoreDb[yierdis-core\n(DB/Value/TTL/maxmemory/command semantics)]
    OffheapApi[yierdis-offheap-api\n(BytesSource/BytesSink/Slice)]
  end

  subgraph Adapters[Netty adapters]
    ProtocolNetty[yierdis-protocol-netty\n(Netty codec + frame/session adapter)]
    OffheapNetty[yierdis-offheap-netty\n(ByteBuf source/sink adapter)]
    Server[yierdis-server\n(pipeline + executor + bootstrap)]
    Client[yierdis-client\n(netty client codec)]
  end

  Protocol --> OffheapApi
  CoreDb --> Protocol
  CoreDb --> OffheapApi

  ProtocolNetty --> Protocol
  ProtocolNetty --> OffheapApi
  ProtocolNetty --> OffheapNetty

  Server --> CoreDb
  Server --> ProtocolNetty
  Client --> ProtocolNetty
```

## Architecture Decision ADR

### ADR-20260115-01: 拆分 protocol 为 Netty-free SSOT + Netty adapter 模块（Recommended）
**Context:** 现状中 protocol/core 直接依赖 Netty 类型，违背知识库约束，且导致边界漂移与可测试性下降。  
**Decision:** 将 Netty codec 与 Channel/ByteBuf 适配移动到独立模块 `yierdis-protocol-netty`，保留 `yierdis-protocol` 仅含 Netty-free 的协议语义 SSOT。  
**Rationale:** 以最小的对外行为变化，最大化边界清晰度；同时保留 server/client/bench 的共享复用能力。  
**Alternatives:**  
- 方案 A：仅更新文档，不改代码 → 拒绝原因：长期误导，技术债累积。  
- 方案 B：在 server 内部私有化全部协议实现 → 拒绝原因：SSOT 失效，client/bench 复用成本上升。  
**Impact:** 引入新模块与一批类移动/重构；需要额外关注对象生命周期与引用计数。

## Security and Performance

- **Security:**
  - 保持 RESP error 输出 CRLF 过滤与限长策略不变
  - 明确 frame/slice 的生命周期边界，避免 UAF 或越界读写
- **Performance:**
  - bytes source/sink 抽象必须支持 memoryAddress fast-path
  - 关键路径避免“ByteBuf → byte[] → off-heap”的双拷贝回退

## Testing and Deployment

- **Testing:**
  - 全量 `mvn test`
  - 补充/调整：协议状态（RESP2↔RESP3）、off-heap 泄漏回归、decoder/encoder 回归
  - 运行 `scripts/smoke.sh`（server/cli/bench strictReplies 链路）
- **Deployment:**
  - Maven module 变更后，确保 `mvn -DskipTests package` 仍可打出 server/client/bench jar
  - README/知识库同步更新模块依赖图与边界描述

