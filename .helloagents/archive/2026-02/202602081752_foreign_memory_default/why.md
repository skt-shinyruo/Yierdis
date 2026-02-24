<!-- migrated_from: history/2026-02/202602081752_foreign_memory_default/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: foreign-memory 默认可用（off-heap foreign）

## Requirement Background

当前对外 CLI/帮助文案已暴露 `--offheapBackend foreign` 选项，但在默认构建下 foreign 后端并不总是可用：

- 构建期：`yierdis-offheap-foreign` 之前仅在 Maven profile `foreign-memory` 启用时才会参与 reactor 构建并进入 server 产物。
- 运行期：在 Java 17 上，Foreign Memory API 位于 incubator 模块 `jdk.incubator.foreign`，需要 `--add-modules jdk.incubator.foreign` 才能被解析到 boot layer。

这会导致用户体验问题：

- 参数“看起来支持”，但默认构建/默认运行参数下会启动失败或提示不可用。
- 部署与脚本需要额外的 profile/运行参数，增加使用门槛。

目标：让 `foreign` 后端在默认构建下可用，并尽量把 `--add-modules` 的复杂度从用户侧拿走；同时保留在非 JDK 17 环境/需要精简构建时的禁用方式。

## Change Content

1. 将 `foreign-memory` profile 设为默认启用（activeByDefault），默认构建编译并打包 `yierdis-offheap-foreign`，并随 `yierdis-server` shaded jar 发布。
2. server 启动期检测到 `--offheapBackend foreign` 且未启用 `jdk.incubator.foreign` 时，自动重启并追加 `--add-modules jdk.incubator.foreign`（并保留原 JVM 参数）。
3. 同步 README 与知识库，更新 foreign 后端的构建/运行说明与退化路径。

## Impact Scope

- **Modules:**
  - `yierdis-offheap` / `yierdis-offheap-foreign`
  - `yierdis-offheap-api`
  - `yierdis-server`
  - `helloagents`（知识库与变更记录）
- **Files:**
  - `yierdis-offheap/pom.xml`
  - `yierdis-server/pom.xml`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java`
  - `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocators.java`
  - `README.md`
  - `helloagents/wiki/modules/offheap.md`
  - `helloagents/CHANGELOG.md`

## Core Scenarios

### Requirement: foreign 后端默认构建可用
**Module:** offheap/server

#### Scenario: 默认构建产物可选择 foreign
条件：用户使用默认构建（`mvn package`），并在运行时选择 `--offheapBackend foreign`。
- 预期：server 产物中包含 `yierdis-offheap-foreign` 实现；无需额外 Maven profile。

### Requirement: foreign 后端运行期零手动参数（尽量）
**Module:** server

#### Scenario: 未显式添加 --add-modules 的启动体验
条件：用户运行 `java -jar ... --offheapBackend foreign`，未显式添加 `--add-modules jdk.incubator.foreign`。
- 预期：server 自动检测模块未启用并重启补齐 `--add-modules`，最终可用。
- 预期：保留原 JVM 参数（例如 `-Xmx`、`-XX:MaxDirectMemorySize`、debug 参数等）。

### Requirement: 可控退化（禁用/非 JDK17 环境）
**Module:** build/runtime

#### Scenario: 显式禁用 foreign-memory profile
条件：用户在构建时使用 `-P!foreign-memory`。
- 预期：不编译/打包 foreign 后端；选择 `--offheapBackend foreign` 时给出明确提示，引导切换后端或重新构建。

## Risk Assessment

- **Risk:** 默认启用 foreign 后端会增加构建对 `jdk.incubator.foreign` 的依赖（JDK 17 预览模块），在非 JDK 17 环境可能编译失败或运行时不可用。
- **Mitigation:**
  - 保留 `-P!foreign-memory` 的禁用方式；
  - 运行期对模块缺失给出明确错误提示；
  - server 仅在用户显式选择 `foreign` 时触发自动补齐/重启逻辑，避免影响默认路径。

