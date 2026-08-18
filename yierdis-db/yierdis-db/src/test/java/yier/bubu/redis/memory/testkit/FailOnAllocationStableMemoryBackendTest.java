package yier.bubu.redis.memory.testkit;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.storage.memory.DbThreadGuard;

public class FailOnAllocationStableMemoryBackendTest {
    @Test
    public void failsExactlyTheConfiguredAllocationAndCanBeReset() {
        try (FailOnAllocationStableMemoryBackend backend = newBackend()) {
            backend.failOnAllocation(2);

            NativeHandle first = backend.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertEquals(new NativeHandle(backend.allocatorId(), 1L), first);
            Assert.assertThrows(
                    NativeCapacityExceededException.class,
                    () -> backend.allocate(NativeObjectKind.STRING_BYTES, 8)
            );
            Assert.assertEquals(2L, backend.allocationAttempts());

            backend.disableFailures();
            NativeHandle resumed = backend.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertEquals(3L, backend.allocationAttempts());
            backend.free(first);
            backend.free(resumed);
        }
    }

    @Test
    public void onlyCapacityGrowingReallocationConsumesAllocationAttempts() {
        try (FailOnAllocationStableMemoryBackend backend = newBackend()) {
            NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 8);
            backend.resetAttempts();
            backend.failOnAllocation(1);

            NativeHandle sameCapacity = backend.reallocate(
                    handle,
                    8,
                    NativeReallocPolicy.PRESERVE_PREFIX
            );
            Assert.assertEquals(0L, backend.allocationAttempts());
            Assert.assertEquals(handle, sameCapacity);

            NativeHandle shrunk = backend.reallocate(
                    sameCapacity,
                    4,
                    NativeReallocPolicy.PRESERVE_PREFIX
            );
            Assert.assertEquals(0L, backend.allocationAttempts());
            Assert.assertEquals(handle, shrunk);

            Assert.assertThrows(
                    NativeCapacityExceededException.class,
                    () -> backend.reallocate(handle, 32, NativeReallocPolicy.PRESERVE_PREFIX)
            );
            Assert.assertEquals(1L, backend.allocationAttempts());
            backend.free(handle);
        }
    }

    private static FailOnAllocationStableMemoryBackend newBackend() {
        HeapStableMemoryBackend delegate =
                new HeapStableMemoryBackend("failure-injection", 8, new DbThreadGuard());
        delegate.bindToCurrentThread();
        return new FailOnAllocationStableMemoryBackend(delegate);
    }
}
