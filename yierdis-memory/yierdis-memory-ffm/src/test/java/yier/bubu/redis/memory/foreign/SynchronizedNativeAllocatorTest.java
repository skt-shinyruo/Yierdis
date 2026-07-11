package yier.bubu.redis.memory.foreign;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class SynchronizedNativeAllocatorTest {
    @Test
    public void twoThreadsCanAllocateAndFreeThroughAdapter() throws Exception {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("synchronized-adapter");
             SynchronizedNativeAllocator allocator = new SynchronizedNativeAllocator(runtime, 128)) {
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread first = worker(allocator, start, failure);
            Thread second = worker(allocator, start, failure);

            start.countDown();
            first.join();
            second.join();

            if (failure.get() != null) {
                throw new AssertionError("concurrent allocator access failed", failure.get());
            }
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    @Test
    public void foreignThreadCannotTerminallyCloseOwnersAllocationScope() throws Exception {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("synchronized-scope-owner");
             SynchronizedNativeAllocator allocator = new SynchronizedNativeAllocator(runtime, 128)) {
            NativeAllocationScope scope = allocator.beginAllocationScope();
            allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread foreign = Thread.ofPlatform().start(() -> {
                try {
                    scope.abort();
                } catch (Throwable expected) {
                    failure.set(expected);
                }
            });
            foreign.join();

            Assert.assertTrue(failure.get() instanceof IllegalStateException);
            scope.abort();
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    @Test
    public void completedScopeCannotClearLaterScopesOwnership() throws Exception {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("synchronized-scope-generation");
             SynchronizedNativeAllocator allocator = new SynchronizedNativeAllocator(runtime, 128)) {
            NativeAllocationScope completed = allocator.beginAllocationScope();
            completed.abort();
            CountDownLatch opened = new CountDownLatch(1);
            CountDownLatch finish = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread owner = Thread.ofPlatform().start(() -> {
                try (NativeAllocationScope ignored = allocator.beginAllocationScope()) {
                    opened.countDown();
                    finish.await();
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            opened.await();

            completed.abort();
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> allocator.allocate(NativeObjectKind.STRING_BYTES, 32)
            );

            finish.countDown();
            owner.join();
            if (failure.get() != null) {
                throw new AssertionError("later scope owner failed", failure.get());
            }
        }
    }

    private static Thread worker(
            SynchronizedNativeAllocator allocator,
            CountDownLatch start,
            AtomicReference<Throwable> failure
    ) {
        return Thread.ofPlatform().start(() -> {
            try {
                start.await();
                for (int i = 0; i < 100; i++) {
                    NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
                    allocator.free(handle);
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
    }
}
