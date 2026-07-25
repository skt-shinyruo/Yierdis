package yier.bubu.redis.storage.memory.internal.value;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;

public class PinnedPoppedValueSequenceTest {
    @Test
    public void emptyHandleSetHasExplicitCapacityBoundary() {
        NativeHandleSet handles = new NativeHandleSet(0);

        Assert.assertEquals(0, handles.size());
        Assert.assertFalse(handles.contains(handle(1)));
        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> handles.add(handle(1))
        );
        Assert.assertEquals("native handle set has zero capacity", failure.getMessage());
    }

    @Test
    public void highCardinalityHandlesUseOneRawLifecycleOperationPerUniqueHandle() {
        int uniqueHandleCount = 4096;
        NativeListEntryRef[] entries = new NativeListEntryRef[uniqueHandleCount * 2];
        for (int index = 0; index < uniqueHandleCount; index++) {
            NativeHandle handle = handle(index + 1L);
            entries[index * 2] = NativeListEntryRef.handle(handle, 1, 32);
            entries[index * 2 + 1] = NativeListEntryRef.handle(handle, 1, 32);
        }

        AtomicInteger pinAttempts = new AtomicInteger();
        AtomicInteger unpinAttempts = new AtomicInteger();
        AtomicInteger freeAttempts = new AtomicInteger();
        StableMemoryBackend allocator = (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[] {StableMemoryBackend.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "pin" -> pinAttempts.incrementAndGet();
                        case "unpin" -> unpinAttempts.incrementAndGet();
                        case "free" -> freeAttempts.incrementAndGet();
                        default -> {
                        }
                    }
                    return null;
                }
        );

        PinnedPoppedValueSequence pinned = PinnedPoppedValueSequence.capture(allocator, entries);
        Assert.assertEquals(uniqueHandleCount, pinAttempts.get());
        Assert.assertEquals((long) uniqueHandleCount * 32L, pinned.retainedMemoryBytes());
        pinned.close();
        Assert.assertEquals(uniqueHandleCount, unpinAttempts.get());

        PreparedPoppedValueSequence prepared = PreparedPoppedValueSequence.owned(allocator, entries);
        Assert.assertEquals(uniqueHandleCount, prepared.retainedHandles().length);
        Assert.assertEquals((long) uniqueHandleCount * 32L, prepared.retainedMemoryBytes());
        prepared.activateOwnership();
        prepared.close();
        Assert.assertEquals(uniqueHandleCount, freeAttempts.get());
    }

    @Test
    public void sharedBlockIsPinnedAndAccountedOnce() {
        AtomicInteger pinAttempts = new AtomicInteger();
        AtomicInteger unpinAttempts = new AtomicInteger();
        StableMemoryBackend allocator = (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[] {StableMemoryBackend.class},
                (proxy, method, args) -> {
                    if ("pin".equals(method.getName())) {
                        pinAttempts.incrementAndGet();
                    }
                    if ("unpin".equals(method.getName())) {
                        unpinAttempts.incrementAndGet();
                    }
                    return null;
                }
        );
        NativeHandle shared = handle(1);
        PinnedPoppedValueSequence sequence = PinnedPoppedValueSequence.capture(
                allocator,
                new NativeListEntryRef[] {
                        NativeListEntryRef.handle(shared, 2, 3, 64),
                        NativeListEntryRef.handle(shared, 8, 4, 64)
                }
        );

        Assert.assertEquals(1, pinAttempts.get());
        Assert.assertEquals(64L, sequence.retainedMemoryBytes());
        sequence.close();
        Assert.assertEquals(1, unpinAttempts.get());
    }

    @Test
    public void poppedLayoutsKeepEqualLocalRawHandlesFromDifferentBackendsDistinct() {
        NativeHandle left = new NativeHandle(11L, 1L);
        NativeHandle right = new NativeHandle(12L, 1L);
        Assert.assertEquals(left.localRaw(), right.localRaw());
        Assert.assertNotEquals(left.allocatorId(), right.allocatorId());

        List<NativeHandle> pinned = new ArrayList<>();
        List<NativeHandle> unpinned = new ArrayList<>();
        List<NativeHandle> freed = new ArrayList<>();
        StableMemoryBackend allocator = (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (proxy, method, args) -> {
                    NativeHandle handle = (NativeHandle) args[0];
                    switch (method.getName()) {
                        case "pin" -> pinned.add(handle);
                        case "unpin" -> unpinned.add(handle);
                        case "free" -> freed.add(handle);
                        default -> {
                        }
                    }
                    return null;
                }
        );
        NativeListEntryRef[] entries = {
                NativeListEntryRef.handle(left, 1, 32),
                NativeListEntryRef.handle(right, 1, 32)
        };

        PinnedPoppedValueSequence pinnedSequence = PinnedPoppedValueSequence.capture(allocator, entries);
        Assert.assertEquals(64L, pinnedSequence.retainedMemoryBytes());
        Assert.assertEquals(Set.of(left, right), new HashSet<>(pinned));
        pinnedSequence.close();
        Assert.assertEquals(Set.of(left, right), new HashSet<>(unpinned));

        PreparedPoppedValueSequence prepared = PreparedPoppedValueSequence.owned(allocator, entries);
        Assert.assertEquals(2, prepared.retainedHandles().length);
        Assert.assertTrue(prepared.retainsHandle(left));
        Assert.assertTrue(prepared.retainsHandle(right));
        Assert.assertEquals(64L, prepared.retainedMemoryBytes());
        prepared.activateOwnership();
        prepared.close();
        Assert.assertEquals(Set.of(left, right), new HashSet<>(freed));
    }

    @Test
    public void preparedOwnershipFreesSharedBlockOnce() {
        AtomicInteger freeAttempts = new AtomicInteger();
        StableMemoryBackend allocator = (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[] {StableMemoryBackend.class},
                (proxy, method, args) -> {
                    if ("free".equals(method.getName())) {
                        freeAttempts.incrementAndGet();
                    }
                    return null;
                }
        );
        NativeHandle shared = handle(1);
        PreparedPoppedValueSequence sequence = PreparedPoppedValueSequence.owned(
                allocator,
                new NativeListEntryRef[] {
                        NativeListEntryRef.handle(shared, 2, 3, 64),
                        NativeListEntryRef.handle(shared, 8, 4, 64)
                }
        );

        Assert.assertEquals(1, sequence.retainedHandles().length);
        Assert.assertEquals(64L, sequence.retainedMemoryBytes());
        sequence.activateOwnership();
        sequence.close();
        Assert.assertEquals(1, freeAttempts.get());
    }

    @Test
    public void closeContinuesAfterAnUnpinErrorAndReportsAllFailures() {
        AtomicInteger unpinAttempts = new AtomicInteger();
        StableMemoryBackend allocator = (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[] {StableMemoryBackend.class},
                (proxy, method, args) -> {
                    if ("unpin".equals(method.getName())) {
                        throw new AssertionError("unpin failure " + unpinAttempts.incrementAndGet());
                    }
                    return null;
                }
        );
        PinnedPoppedValueSequence sequence = PinnedPoppedValueSequence.capture(
                allocator,
                new NativeListEntryRef[] {
                        NativeListEntryRef.handle(handle(1), 1, 1),
                        NativeListEntryRef.handle(handle(2), 1, 1)
                }
        );

        AssertionError failure = Assert.assertThrows(AssertionError.class, sequence::close);

        Assert.assertEquals("unpin failure 1", failure.getMessage());
        Assert.assertEquals(2, unpinAttempts.get());
        Assert.assertEquals(1, failure.getSuppressed().length);
        Assert.assertEquals("unpin failure 2", failure.getSuppressed()[0].getMessage());
    }

    private static NativeHandle handle(long slotId) {
        return new NativeHandle(1L, slotId);
    }
}
