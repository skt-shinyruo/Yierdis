package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class NativeAllocationScopeTest {
    @Test
    public void abortedNativeScopeRestoresCommittedSnapshot() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 128)) {
            allocator.bindToCurrentThread();
            NativeHandle warmPageValue = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            allocator.free(warmPageValue);
            MemoryUsageSnapshot before = allocator.memoryUsage();
            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
                allocator.allocate(NativeObjectKind.ENTRY_RECORD, 56);
                for (int i = 0; i < 20; i++) {
                    allocator.allocate(NativeObjectKind.STRING_BYTES, 70_000);
                }
                Assert.assertTrue(scope.growth().effectiveBytes() > 0);
                scope.abort();
            }
            Assert.assertEquals(before, allocator.memoryUsage());
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }
}
