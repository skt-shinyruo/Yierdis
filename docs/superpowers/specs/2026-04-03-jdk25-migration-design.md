# JDK 25 Migration Design

**Date:** 2026-04-03

## Goal

将 Yierdis 的 Java 基线从 JDK 17 升级到 JDK 25，并保留 `--offheapBackend foreign` 后端可构建、可测试、可运行。

## Confirmed Constraints

- Maven 多模块构建必须切到 JDK 25 基线，而不是“部分模块 25、部分模块 17”。
- `foreign` 后端不能因为升级而被移除或默认禁用。
- 现有 `offheap` 抽象、ServiceLoader provider 发现方式和 CLI 参数语义必须保持兼容。
- 这是一次平台迁移，不应顺手引入无关的模块拆分或运行时行为改写。

## Problem Summary

当前项目把 Java 版本与 Foreign Memory 实现强绑定到了 Java 17 的 incubator API：

- 根 [pom.xml](/home/feng/code/project/Yierdis/pom.xml) 使用 `maven.compiler.release=17`。
- [yierdis-memory/foreign/pom.xml](/home/feng/code/project/Yierdis/yierdis-memory/foreign/pom.xml) 通过 `--add-modules jdk.incubator.foreign` 编译和测试。
- [YierdisForeignOffHeapAllocator.java](/home/feng/code/project/Yierdis/yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocator.java) 直接依赖 `jdk.incubator.foreign.MemoryAccess`、`MemorySegment`、`ResourceScope`。
- [ForeignMemoryAutoModules.java](/home/feng/code/project/Yierdis/yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java) 依赖 Java 17 的“自动补模块并重启”逻辑。
- [README.md](/home/feng/code/project/Yierdis/README.md) 把 `foreign` 后端描述为 Java 17 incubator 特性。

如果只把 `maven.compiler.release` 改成 `25`，`foreign` 模块会直接编译失败，server 的自动重启说明也会变成错误文档。

## Recommended Approach

采用一次性迁移到 JDK 25 正式 FFM API 的方式：

1. 把构建基线统一切到 JDK 25。
2. 将 `foreign` 后端从 `jdk.incubator.foreign` 迁移到 `java.lang.foreign`。
3. 删除 Java 17 专用的 `--add-modules` 构建参数和运行时自动补模块逻辑。
4. 更新 README 中关于 JDK、`foreign` profile、运行方式的说明。

这比“先禁用 foreign，再补迁移”更合适，因为当前 `foreign-memory` profile 默认启用，项目需要保持默认构建完整。

## Alternatives Considered

### Option A: 只改构建版本，禁用 `foreign`

- Pros: 改动最少
- Cons: 改变默认构建能力，`--offheapBackend foreign` 失效，不符合目标

### Option B: 升级到 JDK 25，并迁移 `foreign` 到正式 FFM API

- Pros: 保留完整能力，构建与运行语义一致，文档也能同步收敛
- Cons: 需要改动 API 调用和测试预期

**Recommendation:** Option B。

## Target Design

### Build and Profile Design

- 根 [pom.xml](/home/feng/code/project/Yierdis/pom.xml) 的 `maven.compiler.release` 升级为 `25`。
- `foreign-memory` profile 继续保留默认启用，不改变模块装配方式。
- [yierdis-memory/foreign/pom.xml](/home/feng/code/project/Yierdis/yierdis-memory/foreign/pom.xml) 删除 `--add-modules jdk.incubator.foreign` 的编译和 surefire 配置，因为 JDK 25 的 FFM API 不再需要 incubator 模块开关。

### Foreign Allocator Design

- 用 `java.lang.foreign.Arena` 替代 `ResourceScope` 作为每个 buffer 的生命周期所有者。
- 用 `MemorySegment.get(ValueLayout.JAVA_BYTE, offset)` / `set(...)` 替代 `MemoryAccess.getByteAtOffset(...)` / `setByteAtOffset(...)`。
- 保留现有 allocator 语义：
  - `allocate(int capacity)` 仍然按 `maxBytes` 做上限判断
  - `close()` 仍然在泄漏时抛错
  - `slice()`、`getBytes()`、`setBytes()`、`writeTo()` 的行为保持不变
- `foreign` provider 与 ServiceLoader 发现机制不改。

### Server Runtime Design

- `foreign` 后端在 JDK 25 上不再需要 `--add-modules` 自动补齐，因此 [ForeignMemoryAutoModules.java](/home/feng/code/project/Yierdis/yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java) 需要改成“环境校验器”而不是“重启器”。
- 对 `--offheapBackend foreign`，server 只需要在当前 JVM 无法提供正式 FFM API 时给出稳定、清晰的错误提示；在 JDK 25 上应直接返回 `null`，不触发重启。

### Testing Design

- 在 `yierdis-memory/api` 和 `yierdis-memory/foreign` 侧保留现有 ServiceLoader/allocator 合约测试。
- 增加一条 server 启动侧测试，验证 `foreign` 后端在不需要 incubator 模块时不会触发“自动重启”路径。
- 构建验证使用 JDK 25 显式运行，避免被当前 shell 的 JDK 17 污染。

### Documentation Design

- README 标题、环境说明、off-heap 章节全部改为 JDK 25 口径。
- 删除“不是 JDK 17 时禁用 profile”“需要 `--add-modules jdk.incubator.foreign`”“server 自动重启补模块”等旧说明。
- 明确说明 `foreign` 后端现在基于 JDK 25 正式 FFM API。

## Out of Scope

- 改变 `offheap` 抽象接口
- 修改 `unsafe` / `netty` 后端实现
- 调整 server 的其他启动流程
- 做与 JDK 25 迁移无关的重构
