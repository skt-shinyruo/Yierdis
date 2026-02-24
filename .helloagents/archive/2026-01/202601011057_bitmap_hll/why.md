<!-- migrated_from: history/2026-01/202601011057_bitmap_hll/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# 变更提案：补齐 BITMAP / HyperLogLog（基于 STRING 复用）

## 需求背景

当前 Yierdis 已支持 Redis 风格的核心数据结构与命令子集，但缺少基于 `STRING` 复用的位图（BITMAP）与 HyperLogLog（HLL）命令族，导致部分常用场景无法通过 `redis-cli` 直接验证。

本变更目标是在 **不引入新 ValueType** 的前提下，新增 BITMAP 与 HLL 的命令语义，并尽量贴近 Redis 的结果与行为（以“功能可用”为主）。

## 变更内容

1. 新增 BITMAP 命令：`SETBIT` / `GETBIT` / `BITCOUNT`
2. 新增 HLL 命令：`PFADD` / `PFCOUNT` / `PFMERGE`
3. HLL 存储采用更贴近 Redis 的思路：支持 sparse/dense 两种内部表示，并在需要时互转（实现复杂度更高）

## 影响范围

- **Modules:** command, db, offheap, test
- **Files:** 预计修改 `yierdis-server/src/main/java/yier/bubu/redis/command/*`、`yierdis-server/src/main/java/yier/bubu/redis/db/*`、`yierdis-server/src/main/java/yier/bubu/redis/db/offheap/*`，并新增/更新测试用例
- **APIs:** 新增 RESP2 命令集合（Redis 风格）
- **Data:** 仍存储为 `STRING`，但内容将包含 BITMAP/HLL 的内部二进制结构

## 核心场景

### Requirement: BITMAP 基础命令
**Module:** command/db
提供 `SETBIT/GETBIT/BITCOUNT`，并按 Redis 的 bit 顺序与范围规则工作。

#### Scenario: SETBIT/GETBIT 基本语义
条件：key 不存在或为 string
- 预期：`GETBIT` 对不存在 key 返回 0；`SETBIT` 会按需扩容 string，并返回旧 bit 值

#### Scenario: BITCOUNT 计数
条件：key 不存在或为 string
- 预期：无范围参数时统计全量；带 `start end` 时按 Redis 的 byte-range 规则统计并返回整数

### Requirement: HLL 基础命令
**Module:** command/db
提供 `PFADD/PFCOUNT/PFMERGE`，结果尽量贴近 Redis（以功能可用为主）。

#### Scenario: PFADD 创建与更新
条件：key 不存在或为有效 HLL string
- 预期：key 不存在时创建 HLL；添加元素后返回 1/0（是否有寄存器变化）

#### Scenario: PFCOUNT 与 PFMERGE
条件：若干个 HLL key
- 预期：`PFCOUNT` 返回近似基数；`PFMERGE` 将多个 key 合并到目标 key 后，`PFCOUNT` 与合并前集合并集结果一致（误差与 Redis 尽量贴近）

## 风险评估

- **风险：** `SETBIT` 可能触发大幅扩容（偏移量过大导致内存压力）
  - **缓解：** 对 offset/长度做边界保护；接入 `maxmemory` 检查并在 OOM 策略下返回错误
- **风险：** off-heap（unsafe）字符串目前缺少随机写能力，HLL/BITMAP 需要原地修改
  - **缓解：** 为 `YierdisUnsafeOffHeapString` 补齐 `setByte`/随机写与长度扩容相关接口
- **风险：** HLL sparse/dense 兼容性与实现复杂，容易引入边界 bug
  - **缓解：** 通过 heap/off-heap 双模式测试覆盖关键场景；必要时先实现 dense 再逐步补齐 sparse

