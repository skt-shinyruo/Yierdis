package yier.bubu.redis.storage.memory.internal.value;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class PinnedPoppedValueSequenceTest {
    @Test
    public void emptyRawHandleSetHasExplicitCapacityBoundary() {
        NativeRawHandleSet handles = new NativeRawHandleSet(0);

        Assert.assertEquals(0, handles.size());
        Assert.assertFalse(handles.contains(handle(1).raw()));
        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> handles.add(handle(1).raw())
        );
        Assert.assertEquals("native raw handle set has zero capacity", failure.getMessage());
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
        NativeAllocator allocator = (NativeAllocator) Proxy.newProxyInstance(
                NativeAllocator.class.getClassLoader(),
                new Class<?>[] {NativeAllocator.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "pinRaw" -> pinAttempts.incrementAndGet();
                        case "unpinRaw" -> unpinAttempts.incrementAndGet();
                        case "freeRaw" -> freeAttempts.incrementAndGet();
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
        NativeAllocator allocator = (NativeAllocator) Proxy.newProxyInstance(
                NativeAllocator.class.getClassLoader(),
                new Class<?>[] {NativeAllocator.class},
                (proxy, method, args) -> {
                    if ("pin".equals(method.getName()) || "pinRaw".equals(method.getName())) {
                        pinAttempts.incrementAndGet();
                    }
                    if ("unpin".equals(method.getName()) || "unpinRaw".equals(method.getName())) {
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
    public void preparedOwnershipFreesSharedBlockOnce() {
        AtomicInteger freeAttempts = new AtomicInteger();
        NativeAllocator allocator = (NativeAllocator) Proxy.newProxyInstance(
                NativeAllocator.class.getClassLoader(),
                new Class<?>[] {NativeAllocator.class},
                (proxy, method, args) -> {
                    if ("free".equals(method.getName()) || "freeRaw".equals(method.getName())) {
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
        NativeAllocator allocator = (NativeAllocator) Proxy.newProxyInstance(
                NativeAllocator.class.getClassLoader(),
                new Class<?>[] {NativeAllocator.class},
                (proxy, method, args) -> {
                    if ("unpin".equals(method.getName()) || "unpinRaw".equals(method.getName())) {
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
        return NativeHandle.of(
                NativeObjectKind.STRING_BYTES.domain(),
                NativeObjectKind.STRING_BYTES,
                slotId,
                1,
                0
        );
    }
}
