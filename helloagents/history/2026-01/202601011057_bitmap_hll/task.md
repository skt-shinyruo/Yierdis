# Task List: BITMAP / HyperLogLog（复用 STRING）

Directory: `helloagents/plan/202601011057_bitmap_hll/`

---

## 1. db（STRING 扩展 + BITMAP/HLL 核心）

- [√] 1.1 在 `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisObject.java` 实现 STRING 随机读写/按需扩容能力，支持 BITMAP/HLL 的原地修改（verify why.md#requirement-bitmap-基础命令-scenario-setbitgetbit-基本语义）
- [√] 1.2 在 `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java` 增加 BITMAP API（`setBit/getBit/bitcount`）并处理 TTL/WRONGTYPE/maxmemory（verify why.md#requirement-bitmap-基础命令-scenario-bitcount-计数）
- [√] 1.3 新增 `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`（或同目录命名）实现 HLL（sparse/dense、packed registers、merge、count），并在 `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java` 暴露 `pfadd/pfcount/pfmerge`（verify why.md#requirement-hll-基础命令）

## 2. command（命令路由与参数解析）

- [√] 2.1 在 `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java` 增加 `SETBIT/GETBIT/BITCOUNT/PFADD/PFCOUNT/PFMERGE` 的路由、参数解析与回复写出（verify why.md#核心场景）
- [-] 2.2 在 `yierdis-server/src/main/java/yier/bubu/redis/command/CommandProcessor.java` 增加同样命令，保证与 fast processor 语义一致（verify why.md#requirement-bitmap-基础命令 / #requirement-hll-基础命令）
  > Note: 当前代码库已移除对象式 `CommandProcessor` 线上路径，并以 `YierdisFastCommandProcessor` 作为 SSOT；目标文件不存在，因此该任务跳过。

## 3. offheap（随机写支持）

- [√] 3.1 在 `yierdis-server/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapString.java` 补齐 `setByte` 等随机写能力，并确保扩容后可安全写入任意 index（verify why.md#风险评估）

## 4. Security Check

- [√] 4.1 执行安全检查：参数校验、超大内存分配保护、maxmemory 逻辑接入、off-heap 资源释放一致性（per G9）

## 5. Documentation Update（知识库同步）

- [√] 5.1 更新 `helloagents/wiki/modules/command.md` 与 `helloagents/wiki/api.md`，补充新命令与边界说明
- [√] 5.2 更新 `helloagents/CHANGELOG.md`

## 6. Testing

- [√] 6.1 新增 `yierdis-server/src/test/java/yier/bubu/redis/command/BitmapCommandTest.java` 覆盖 `SETBIT/GETBIT/BITCOUNT`（heap/off-heap）
- [√] 6.2 新增 `yierdis-server/src/test/java/yier/bubu/redis/command/HllCommandTest.java` 覆盖 `PFADD/PFCOUNT/PFMERGE`（heap/off-heap）
