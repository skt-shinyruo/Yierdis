# Change Proposal: 模块命名对齐与协议层拆分（protocol-model / protocol-codec）

## Requirement Background

- 现状：`yierdis-server` 模块目录为 `yierdis-server/`，但其发布坐标（artifactId）为 `yierdis`，导致依赖声明、制品定位与排障（jar 名称、依赖树）出现“名称不一致”。
- 现状：`yierdis-protocol` 同时包含：
  - JSON 解析/写出（`yier.bubu.redis.protocol.json.*`）
  - Custom Protocol v1 NDJSON reply 编码（`yier.bubu.redis.protocol.v1.*`）
  - Reply IR 模型（`yier.bubu.redis.protocol.reply.*`）
  - 协议无关抽象与输出端口（`Command/ReplyWriter/Session/...`）
  这使得 core 在依赖“抽象端口/模型”时被迫引入 codec 实现，模块边界过宽，后续替换协议/编码或做 embedded 复用的成本会被放大。

## Change Content

1. **服务端制品坐标对齐**：将 `yierdis-server` 的 `artifactId` 从 `yierdis` 调整为 `yierdis-server`，并同步更新依赖引用与运行文档，确保“目录名/模块名/制品名”一致。
2. **协议层拆分**：
   - 新增 `yierdis-protocol-model`：承载协议无关抽象（`Command/ReplyWriter/ReplySink/Session/TransactionState/ProtocolLimits/BuildInfo` 等）与 Reply IR（`ReplyValue/*`）。
   - 新增 `yierdis-protocol-codec`：承载 JSON codec（`JsonParser/JsonWriter/...`）与 Custom Protocol v1 编解码（NDJSON encoder、默认 reply writer factory 等）。
   - 将 `yierdis-protocol` 变更为**兼容聚合层**（对外保留原坐标，内部仅聚合依赖），降低仓库内外迁移成本。
3. **依赖方向收敛**：`yierdis-core` 仅依赖 `yierdis-protocol-model`；`yierdis-protocol-netty` 依赖 `yierdis-protocol-codec`；`yierdis-server` 依赖 `yierdis-protocol-netty` + 必要的 codec 实现（用于默认 `ReplyWriterFactory`），避免 core 引入 codec。

## Impact Scope

- **Modules:**
  - `yierdis-server`（坐标调整）
  - `yierdis-client`（测试依赖修正，文档/运行 jar 名称变更）
  - `yierdis-core`（依赖从 `yierdis-protocol` → `yierdis-protocol-model`）
  - `yierdis-protocol`（职责收敛为兼容聚合层）
  - `yierdis-protocol-model`（新增）
  - `yierdis-protocol-codec`（新增）
  - `yierdis-protocol-netty`（依赖调整）
  - `pom.xml`（父模块 modules 列表 + 依赖/版本管理）
- **Files:** 多个 `pom.xml` + `yierdis-protocol*` 相关 Java 源文件迁移 + README/知识库文档更新
- **APIs:** Java 公共 API（包名保持不变，主要为 Maven 坐标与类所在 artifact 的变化）
- **Data:** None

## Core Scenarios

<a id="server-artifactid-align"></a>
### Requirement: 服务端 artifactId 对齐
**Module:** yierdis-server
将服务端模块制品坐标与目录/模块名一致，便于依赖、发布与排障。

<a id="server-build-and-run"></a>
#### Scenario: 构建与运行
条件：执行 `mvn package`
- 预期：`yierdis-server/target/` 产物以 `yierdis-server-<version>.jar` 命名
- 预期：README/文档中的启动命令同步更新
- 预期：项目内对旧 `artifactId=yierdis` 的依赖引用被替换为 `yierdis-server`

<a id="protocol-split-model-codec"></a>
### Requirement: protocol 拆分为 model/codec
**Module:** yierdis-protocol-*
降低 core 与具体协议/编码实现的耦合，收敛模块边界，降低后续替换协议/编码或 embedded 复用的改动半径。

<a id="core-model-only"></a>
#### Scenario: core 无 codec 依赖
条件：`yierdis-core` 仅依赖 `yierdis-protocol-model`
- 预期：core 仍可编译与运行（命令层仍使用 `Command/ReplyWriter/ReplyValue` 等抽象/模型）
- 预期：`yierdis-protocol-codec` 的 JSON / v1 encoder 不被 core 直接依赖

<a id="server-netty-works"></a>
#### Scenario: server/netty 继续工作
条件：server 通过 `yierdis-protocol-netty` 解码请求，并通过 codec 输出 NDJSON reply
- 预期：现有集成测试（例如 resync/backpressure/fair scheduling）仍可通过
- 预期：协议语义保持不变（wire 协议与 error envelope 不变）

## Risk Assessment

- **Risk:** Maven 坐标变更可能影响外部依赖方与内部测试；协议模块拆分可能引入依赖树变化与类路径问题。
- **Mitigation:** 保留 `yierdis-protocol` 作为兼容聚合层；在仓库内统一迁移依赖到新坐标；以 `mvn test` + 关键集成测试为回归基线，并同步更新知识库中的依赖方向与模块边界说明。

