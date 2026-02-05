# 202602050920_offheap_free_list_debug - Lightweight Iteration

目标：将 off-heap allocator free list 指针破坏从 JVM SIGSEGV 转换为可定位的 Java 异常，并提供可选 debug 追踪能力。

## Tasks

- [√] 分析 `hs_err_pid*.log`：确认崩溃位于 `PlatformDependent.getByte`，根因是 free list next 指针被覆盖导致非法地址解引用
- [√] 在 `YierdisUnsafeOffHeapAllocator` 增加 free list 指针合理性校验（避免直接 SIGSEGV）
- [√] 增加可选调试记录（`-Dyierdis.offheap.debug=true`）：记录 alloc/free 调用栈与“期望 next 指针”，next 被覆盖时抛出带 cause/suppressed 的异常
- [√] 更新知识库：补充 offheap-unsafe free list 诊断说明
- [√] 更新知识库 CHANGELOG：记录本次加固变更

## Verification

- [√] `mvn test -pl yierdis-offheap/unsafe -am -Dsurefire.failIfNoSpecifiedTests=false`
- [√] `mvn test -pl yierdis-offheap/unsafe -am -Dsurefire.failIfNoSpecifiedTests=false -Dyierdis.offheap.debug=true`
- [√] `mvn test -pl yierdis-core -am -Dtest=HashCommandTest#hashUpgradesAfterManyFieldsAndKeepsData -Dsurefire.failIfNoSpecifiedTests=false -Dyierdis.offheap.debug=true`（多次重复运行未复现 SIGSEGV）

