<!-- migrated_from: history/2026-02/202602050900_zero_copy_command_args/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Zero-Copy Command Args (Low Allocation Hot Path)

Directory: `helloagents/plan/202602050900_zero_copy_command_args/`

---

## Tasks
- [√] 1. 消除数字解析路径对 `RespCommand.toByteArray()` 的依赖（`parseLong/parseNonNegativeLong/parseIntClamped`）
- [√] 2. `KEYS/SCAN`：pattern 走 `YierdisBytesView`；返回 keys 走 `KeyHandle` 并以 `BytesSlice` 零拷贝写回
- [√] 3. Multi-key：`DEL/PFCOUNT/PFMERGE` 去除 `sliceResetFromCommand` 的 `byte[]` 拷贝与临时 `List<byte[]>`
- [√] 4. 新增 `offHeapKeysEnabled=true` 场景命令级 smoke 覆盖（KEYS/SCAN/DEL/PFCOUNT/PFMERGE）
- [√] 5. 全量回归：`mvn test` 通过

