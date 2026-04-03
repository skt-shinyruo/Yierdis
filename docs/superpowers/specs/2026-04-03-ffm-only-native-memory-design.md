# FFM-Only Native Memory Design

**Date:** 2026-04-03

## Goal

将 Yierdis 的内存模型收敛为 JDK 25 `java.lang.foreign` / FFM 单一路径：

- 删除 `netty` 和 `unsafe` 两套 off-heap backend
- 删除 `--offheapBackend`、`--offheapKeysEnabled`、`--offheapMaxBytes`
- 不再支持 `none` 模式
- keyspace、expires、string/hash/list/set/zset/hll 等内部结构默认全部使用 FFM native memory
- native memory 统一纳入现有 `maxmemory` 治理，而不是再维护一套独立的 off-heap 配额

## Confirmed Constraints

- 这是一次允许破坏内部 API 的系统性重构，不要求保留旧的内部抽象形状。
- CLI 上必须彻底删除 `unsafe`、`netty`、`none` 相关配置语义，而不是仅做废弃提示。
- 最终只允许一种 native memory 实现：JDK 25 FFM。
- keyspace、expires、hash/listpack 等内部结构必须默认走 FFM，不允许保留“值在 off-heap、索引在 heap”这类分裂模式。
- 所有值类型的底层字节存储都需要统一迁移到 FFM；Java 对象只保留协调和索引职责。

## Problem Summary

当前仓库虽然已经完成 JDK 25 基线升级，并把 `foreign` backend 迁移到 `java.lang.foreign`，但整体架构仍停留在“多 backend + 渐进迁移”阶段：

- `yierdis-memory` 仍同时包含 `api`、`netty`、`unsafe`、`foreign` 多套模块。
- CLI 和运行时配置仍暴露 `--offheapBackend none|netty|unsafe|foreign`、`--offheapKeysEnabled`、`--offheapMaxBytes`。
- `YierdisDb` 仍通过 `OffHeapAddressAllocator` 决定是否启用 off-heap keyspace / expire index。
- `KeyHandle` 的 off-heap 身份仍建立在 `(allocator, address, len)` 上。
- `HashValue`、`ListValue`、`SetValue`、`ZSetValue` 仍大量依赖 `unsafe` 风格的裸地址结构。
- HLL 虽然复用 string bytes，但 string payload 仍保留 `byte[]`、`OffHeapBuf`、`YierdisUnsafeOffHeapString` 三轨并存的历史兼容逻辑。

这套结构和目标状态不一致。只删除旧模块而保留现有抽象，会留下大量“多 backend 时代”的空壳：

- `YierdisOffHeapBackend`
- `YierdisOffHeapAllocators`
- `YierdisOffHeapAllocatorProvider`
- `OffHeapAddressAllocator`
- `YierdisUnsafeOffHeap*` 系列类型

如果继续围绕这些抽象补适配层，FFM 最终仍会被压回裸地址模型，生命周期管理、重定位策略、测试矩阵和错误语义都会继续复杂化。

## Recommended Approach

采用“FFM 成为默认且唯一内存模型”的方式重构：

1. 将 native memory 从“可选 backend”提升为 DB 的默认基础设施。
2. 删除多 backend 发现、切换和 fallback 语义。
3. 用 FFM-only 的内部原语替代裸地址和 `OffHeapAddressAllocator`。
4. 将 keyspace、expires 和所有值类型统一收敛到共享的 FFM 数据结构组件。
5. 将 native memory 记账直接并入 `maxmemory` / `MemoryLedger`。

## Alternatives Considered

### Option A: 只删除 `unsafe` / `netty` 模块，保留现有多 backend 抽象

- Pros: 表面改动较少
- Cons: 会保留错误抽象边界，后续仍需第二轮重构；FFM 仍被迫适配 `address + allocator` 模型

### Option B: 保留 backend 概念，但只允许 `none | foreign`

- Pros: 对外迁移相对平滑
- Cons: 仍保留“是否启用 off-heap”的双模系统，DB、测试和文档继续维护两套路径

### Option C: 删除 backend/toggle 语义，FFM 成为默认且唯一 native memory 实现

- Pros: 与目标完全一致；系统边界最清晰；后续代码复杂度最低
- Cons: 改动面最大，需要一次性梳理 CLI、运行时、DB 内部结构和测试矩阵

**Recommendation:** Option C。

## Target Design

### 1. System-Level Simplification

- 删除 `--offheapBackend`、`--offheapKeysEnabled`、`--offheapMaxBytes`。
- `YierdisServerRuntimeConfig` 不再携带 backend 选择与 off-heap toggle。
- `yierdis-memory` 聚合模块只保留 `api` 与 `foreign`。
- 删除 `foreign-memory` profile，默认构建始终包含 FFM 实现。
- 删除 `yierdis-memory-netty`、`yierdis-memory-unsafe` 及其 provider、测试、文档和依赖装配。
- `YierdisOffHeapBackend`、`YierdisOffHeapAllocators`、`YierdisOffHeapAllocatorProvider` 下线，native memory 初始化改为固定 FFM runtime。

### 2. FFM Runtime as SSOT

引入一组 FFM-only 的内部原语，替代裸地址与多 backend 抽象：

- `NativeRegion`
  - 拥有型连续内存块
  - 内部持有 `Arena` 与 `MemorySegment`
  - 负责 deterministic close 和容量记账

- `NativeSpan`
  - 非拥有型视图
  - 基于 `MemorySegment.asSlice(...)`
  - 用于 key bytes、value bytes、slot window、listpack entry 等局部访问

- `NativeAccess`
  - 封装 `byte/int/long/varint` 读写、比较、复制、填充
  - 禁止业务代码直接依赖裸 offset 公式

- `NativeMemoryRuntime`
  - DB 级 native memory 入口
  - 负责分配、释放、命名对象、泄漏检查和与 `MemoryLedger` 集成

`YierdisDb` 不再接收“可选 off-heap allocator”作为插件依赖，而是固定依赖 `NativeMemoryRuntime`。

### 3. Key Identity, Keyspace, and Expires

- `KeyHandle` 从 `(allocator, address, len)` 身份模型收敛为 native bytes 引用模型。
- 新的 off-heap key 句柄内部只持有 `NativeBytesRef` 或等价的 `NativeSpan + metadata`。
- `KeyHandleAccess` 仅暴露内部 span/ref，不再暴露裸地址。

keyspace 采用 FFM-only 的开放寻址哈希表实现：

- 槽位元数据放入 native arrays
  - `states`
  - `hashes`
  - `keyRefIds` / `offsets` / `lengths` 或等价字段
- 值对象引用保留在 Java `Object[]` 中
- key bytes 不直接塞在槽位结构中，而是托管给 DB 级 `NativeBlobStore`

expires 也使用独立的 FFM 哈希表，但共享同一份 key bytes 引用：

- 不再复制 key bytes
- 只保存 key ref 和 `expireAtMillis`
- 删除 key 时由 `YierdisDb` 统一协调 keyspace、expires 与 blob 引用释放顺序

这样 keyspace 与 expires 的正确性不再依赖“共享 allocator + 稳定 address”的旧假设。

### 4. Value Storage Model

所有值类型默认使用 FFM native memory：

- `STRING`
  - 保留 `STRING_INT / STRING_EMBSTR / STRING_RAW` 逻辑编码
  - `STRING_INT` 仍保留为 Java `long`
  - `EMBSTR` / `RAW` 统一收敛为 `NativeString`

- `HASH`
  - 小对象编码使用 FFM `NativeListpack`
  - 升级后使用 FFM `NativeDict`
  - field/value bytes 全部驻留 native memory

- `LIST`
  - 小列表使用 FFM `NativeListpack`
  - 大列表升级为 FFM `NativeQuickList`

- `SET`
  - 整数集合使用 FFM `NativeIntSet`
  - 超阈值或出现非整数成员后升级为 FFM `NativeDict`

- `ZSET`
  - 小 zset 使用 FFM packed 表示
  - 升级后使用 FFM `NativeZSet`
  - 内部由 native dict + native skiplist 组成

- `HLL`
  - 继续复用 string 语义
  - HLL header、dense/sparse payload 直接存放在 `NativeString`

Java 对象只保留协调职责：

- value type / encoding 元数据
- 指向 native 结构的轻量 payload 引用
- 必要的缓存与统计字段

不再保留 `byte[]`、`OffHeapBuf`、`YierdisUnsafeOffHeapString` 并存的历史兼容路径。

### 5. Memory Governance

- 删除 `offheapMaxBytes` 语义。
- native memory 分配统一纳入 `MemoryLedger` 和 `maxmemory`。
- `maxmemory` 成为唯一硬约束。
- 所有 native 分配都必须在分配前预留、提交后入账、失败时回滚。
- shutdown 时继续执行 native memory 泄漏校验，但报告对象改为 FFM region / native structure 名称，而不是地址摘要。

### 6. Startup and Failure Semantics

- 启动时只校验一次当前 JVM 是否支持 `java.lang.foreign`。
- 若运行环境不满足 JDK 25 FFM 要求，则直接报错退出。
- 不再做自动重启，不再做 backend fallback。
- 运行期 native allocation 失败统一归入 `maxmemory` / ledger 失败语义。

### 7. Build and Distribution

- 从 Maven reactor 中移除 `yierdis-memory/netty`、`yierdis-memory/unsafe`。
- 移除 server/runtime 对旧 memory backend 模块的依赖。
- 更新 README、bench、启动脚本和帮助输出：
  - 不再提及 `none/netty/unsafe`
  - 明确要求 JDK 25
  - 默认内存模型即 FFM

## Migration Sequence

建议按以下顺序实施：

1. 引入 FFM SSOT 原语与 `NativeMemoryRuntime`
2. 先迁 `STRING` 与 HLL
3. 迁 `KeyHandle`、keyspace、expires
4. 迁 `HASH`、`LIST`、`SET`、`ZSET`
5. 删除旧的 `unsafe` / `netty` backend 与相关抽象
6. 收敛 CLI、文档、bench 和测试矩阵

这个顺序的目的：

- 先建立统一内存原语，避免每个值类型重复发明 native 访问层
- 先拿下 string/HLL，给其它复合类型提供稳定的 bytes 基础设施
- 再迁 keyspace/expires，统一对象生命周期和 key identity
- 最后删旧世界，降低过渡期交叉依赖风险

## Testing Design

测试矩阵收敛为 FFM-only：

- 原语级测试
  - `NativeRegion`
  - `NativeSpan`
  - `NativeAccess`
  - native memory accounting / leak detection

- DB 结构测试
  - keyspace rehash / delete / scan
  - expires set/remove/cleanup
  - key handle equality / hashing / lifecycle

- 值类型测试
  - string overwrite / append / zero-copy read path
  - hash/list/set/zset 编码升级
  - HLL sparse/dense 读写与 merge

- 集成测试
  - eviction / expire / shutdown 无 native leak
  - server 启动在 JDK 25 下正常
  - CLI 帮助与配置解析不再出现旧参数

需删除或替换以下测试族：

- `unsafe` backend 专用测试
- `netty` backend 专用测试
- 基于 `--offheapBackend` / `--offheapKeysEnabled` / `--offheapMaxBytes` 的 CLI 测试

## Out of Scope

- 不在此次设计中要求引入新的外部序列化格式兼容层。
- 不在此次设计中顺手做 unrelated 模块边界重构。
- 不要求保留旧的 internal address-based helper API 供第三方复用。

## Expected Outcome

重构完成后，Yierdis 的内存架构应满足以下状态：

- JDK 25 FFM 是唯一 native memory 实现
- 不再存在 backend 选择、off-heap toggle、off-heap 独立配额
- keyspace、expires 和所有值类型默认使用 FFM
- native memory 生命周期、记账、错误语义和测试矩阵全部统一
- 删除 `unsafe` / `netty` 遗留路径后，DB 与运行时代码不再携带“渐进迁移时代”的抽象噪音
