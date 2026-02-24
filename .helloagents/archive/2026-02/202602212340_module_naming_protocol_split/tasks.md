<!-- migrated_from: history/2026-02/202602212340_module_naming_protocol_split/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: 模块命名对齐与协议层拆分（protocol-model / protocol-codec）

Directory: `helloagents/plan/202602212340_module_naming_protocol_split/`

---

## 1. Maven 坐标对齐（server）

- [√] 1.1 将 `yierdis-server/pom.xml` 的 `artifactId` 从 `yierdis` 改为 `yierdis-server`（同步 `<name>`/shade 输出），verify why.md#server-artifactid-align, why.md#server-build-and-run
- [√] 1.2 仓库内替换对旧坐标 `yierdis` 的依赖引用（例如 `yierdis-client/pom.xml` 测试依赖），verify why.md#server-build-and-run，depends on task 1.1
- [√] 1.3 同步更新运行文档（`README.md` + 相关知识库模块文档），verify why.md#server-build-and-run，depends on task 1.1

## 2. 新增模块：`yierdis-protocol-model`

- [√] 2.1 新增 Maven module `yierdis-protocol-model`（pom + 目录结构），并在根 `pom.xml` 注册 modules 顺序，verify why.md#protocol-split-model-codec, why.md#core-model-only
- [√] 2.2 迁移协议无关抽象与 Reply IR 到 `yierdis-protocol-model`（保持包名不变，尽量不改 public API），verify why.md#core-model-only，depends on task 2.1
- [√] 2.3 将 `yierdis-core` 的依赖从 `yierdis-protocol` 调整为 `yierdis-protocol-model`，并修正编译/测试，verify why.md#core-model-only，depends on task 2.2

## 3. 新增模块：`yierdis-protocol-codec`

- [√] 3.1 新增 Maven module `yierdis-protocol-codec`（依赖 `yierdis-protocol-model` + `yierdis-bytes`），verify why.md#protocol-split-model-codec, why.md#server-netty-works
- [√] 3.2 迁移 JSON codec（`json/*`）与 Custom Protocol v1 codec（`v1/*`，含 NDJSON encoder 与默认 reply writer factory）到 `yierdis-protocol-codec`，verify why.md#server-netty-works，depends on task 3.1
- [√] 3.3 将 `yierdis-protocol` 调整为兼容聚合层（聚合依赖 `protocol-model` + `protocol-codec`，并移除/迁移其源码），verify why.md#protocol-split-model-codec，depends on task 3.2

## 4. 适配层与应用依赖迁移

- [√] 4.1 将 `yierdis-protocol-netty` 的依赖从 `yierdis-protocol` 改为 `yierdis-protocol-codec`，并修正编译/测试，verify why.md#server-netty-works，depends on task 3.2
- [√] 4.2 修正 `yierdis-server` 中对 `JsonLineReplyWriterFactory/CustomCommand` 等类型的引用（如存在变更），确保 server 仍能装配默认 reply writer，verify why.md#server-netty-works，depends on task 4.1
- [√] 4.3 视需要调整 `yierdis-client`/`yierdis-bench` 的依赖与引用（确保不意外回退到聚合层依赖），verify why.md#server-netty-works，depends on task 4.1
- [√] 4.4 将 `yierdis-args` 的依赖从 `yierdis-protocol` 调整为 `yierdis-protocol-model`（避免 args 无意引入 codec），depends on task 3.3

## 5. Security Check

- [√] 5.1 执行安全检查（G9）：确认 core 不依赖 codec；确认 `ProtocolLimits/JsonLimits` 与错误消息净化仍为 SSOT 且行为不变；确认无新增敏感信息输出

## 6. 文档与知识库同步

- [√] 6.1 更新 `helloagents/wiki/arch.md` 的模块图与依赖方向（新增 `protocol-model`/`protocol-codec`），depends on task 3.3
- [√] 6.2 更新 `helloagents/wiki/modules/protocol.md`（拆分后职责边界与依赖说明），depends on task 3.3
- [√] 6.3 更新 `helloagents/wiki/modules/server.md`（artifactId 对齐后的依赖与运行说明），depends on task 1.3
- [√] 6.4 更新 `helloagents/CHANGELOG.md` 记录本次 refactor 的 Added/Changed，depends on task 6.1

## 7. Testing

- [√] 7.1 运行全量测试 `mvn test` 并修正回归问题（仅限本次改动范围内），depends on tasks 1.3, 4.3, 6.4
- [-] 7.2 构建并做最小冒烟（可选）：启动 server + CLI 执行 `PING/SET/GET/INFO/STATS`，验证协议行为未变，depends on task 7.1
  > Note: 已通过 `mvn test` 覆盖 server/client 的集成测试基线，本轮未额外执行手动启动冒烟。
