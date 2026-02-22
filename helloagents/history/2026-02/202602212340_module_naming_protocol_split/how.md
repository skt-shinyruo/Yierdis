# Technical Design: 模块命名对齐与协议层拆分（protocol-model / protocol-codec）

## Technical Solution

### Core Technologies

- Java 17 + Maven 多模块
- Netty（server/client）
- Custom Protocol v1（`<len>:<json>\\n` request + NDJSON reply）

### Implementation Key Points

- **坐标治理（server）**：将 `yierdis-server` 的发布坐标改为 `artifactId=yierdis-server`，并同步修正仓库内依赖与运行文档，减少“目录/制品名不一致”的排障成本。
- **协议层分层（protocol）**：将 `yierdis-protocol` 拆分为：
  - `yierdis-protocol-model`：抽象端口 + Reply IR 模型（core 只依赖此模块）。
  - `yierdis-protocol-codec`：JSON codec + Custom Protocol v1 编解码实现（Netty adapter / server 使用）。
  - `yierdis-protocol`：兼容聚合层（依赖 model + codec；用于平滑迁移与对外兼容）。
- **依赖方向收敛**：以 Maven 依赖为“硬边界”，让 core 在构建层面无法“意外”依赖 codec（避免未来协议替换/编码演进时改动半径扩大）。
- **迁移策略（分步、可回退）**：
  1. 先引入新模块（model/codec）与对应 `pom.xml`。
  2. 将 `yierdis-protocol` 的 Java 源文件按职责迁移到新模块（不改包名，尽量不改 public API）。
  3. 逐个模块替换依赖：core → model；protocol-netty/server/client → codec 或保留聚合层。
  4. 最后将 `yierdis-protocol` 降级为兼容聚合层（仅保留 pom 与依赖聚合），并在文档中明确推荐依赖路径。

## Architecture Design

```mermaid
flowchart LR
  subgraph SSOT[SSOT Modules]
    Bytes[yierdis-bytes]
    Model[yierdis-protocol-model]
    Core[yierdis-core]
  end

  subgraph Codec[Protocol Codec]
    CodecImpl[yierdis-protocol-codec]
    ProtocolAgg[yierdis-protocol (compat)]
  end

  subgraph Adapters[Adapters]
    BytesNetty[yierdis-bytes-netty]
    ProtocolNetty[yierdis-protocol-netty]
  end

  subgraph Apps[Apps]
    Server[yierdis-server]
    Client[yierdis-client]
    Bench[yierdis-bench]
  end

  CodecImpl --> Model
  ProtocolAgg --> Model
  ProtocolAgg --> CodecImpl

  Model --> Bytes
  Core --> Model

  ProtocolNetty --> CodecImpl
  ProtocolNetty --> BytesNetty

  Server --> ProtocolNetty
  Server --> Core
  Client --> ProtocolNetty
  Bench --> ProtocolNetty
```

## Architecture Decision ADR

### ADR-20260221-01: 拆分 `yierdis-protocol` 为 `protocol-model` / `protocol-codec`，并保留兼容聚合层

<a id="adr-20260221-01"></a>

**Context:** 当前 `yierdis-protocol` 同时承载“抽象端口/Reply IR 模型”与“具体 codec 实现（JSON + v1 NDJSON）”，导致 core 在依赖端口时被动引入 codec，模块边界过宽，替换协议/编码与 embedded 复用的成本随演进放大。  
**Decision:** 新增 `yierdis-protocol-model` 与 `yierdis-protocol-codec`，并将 `yierdis-protocol` 调整为兼容聚合层（对外保留坐标、内部聚合依赖）。  
**Rationale:** 用 Maven 依赖边界硬化“core 不依赖 codec”的架构约束；同时保留 `yierdis-protocol` 作为聚合层降低迁移成本与外部破坏性。  
**Alternatives:**
- 方案 A：不拆 module，仅用约束/文档收敛边界 → Rejection reason: 约束偏软，无法从依赖树层面阻断漂移。
- 方案 B：将输出端口下沉到 `core-api`，codec 全部留在 protocol → Rejection reason: 解耦更强但改动面更大，且需要引入新的 core API 模块与依赖重排（可作为后续演进方向）。  
**Impact:** 模块数增加（2 个），但依赖方向更清晰；对外坐标可兼容；内部迁移需要一次性移动源码与调整依赖声明。

### ADR-20260221-02: `yierdis-server` 制品坐标与模块名对齐（artifactId 改为 `yierdis-server`）

<a id="adr-20260221-02"></a>

**Context:** 当前 `yierdis-server` 目录名与发布坐标不一致（`artifactId=yierdis`），对依赖声明、制品定位、排障不友好。  
**Decision:** 将服务端模块发布坐标改为 `artifactId=yierdis-server`，并同步修正仓库内引用与 README/知识库文档。  
**Rationale:** 与仓库内其他模块（`yierdis-core/yierdis-client/...`）保持一致的命名与定位方式。  
**Alternatives:** 仅重命名目录或仅重命名模块显示名 → Rejection reason: 无法解决依赖/制品层面的不一致。
**Impact:** 旧坐标依赖方需要迁移（仓库内会统一替换）；jar 名称发生变化，需同步更新运行命令与文档。

## Security and Performance

- **Security:**
  - 不改变 Custom Protocol v1 wire 协议语义；保持既有 `ProtocolLimits/JsonLimits` 等 DoS 防护上限与错误消息净化逻辑。
  - 通过依赖方向收敛，降低“core 误用 codec 细节”导致的边界绕过风险（例如未来引入非预期的解析路径）。
- **Performance:**
  - 该拆分属于模块/依赖治理，原则上不引入新的运行时开销。
  - 迁移过程中需确保 `CustomProtocolV1NdjsonEncoder` 的 streaming fast-path 保持不变（避免无意间引入 heap `byte[]` 分配或多余 copy）。

## Testing and Deployment

- **Testing:**
  - 基线：`mvn test`（全模块）。
  - 重点回归：server 的 backpressure/fair scheduling/resync 集成测试；protocol-netty 的 decoder 单测；core 的命令注册/Reply IR 相关测试。
- **Deployment:**
  - 对内：仓库内统一迁移依赖坐标后发布。
  - 对外：如已有外部依赖方，建议在发布说明中标注坐标变化与迁移方式；`yierdis-protocol` 保留聚合层以降低迁移成本。
