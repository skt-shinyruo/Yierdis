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
    public void closeContinuesAfterAnUnpinErrorAndReportsAllFailures() {
        AtomicInteger unpinAttempts = new AtomicInteger();
        NativeAllocator allocator = (NativeAllocator) Proxy.newProxyInstance(
                NativeAllocator.class.getClassLoader(),
                new Class<?>[] {NativeAllocator.class},
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
        return NativeHandle.of(
                NativeObjectKind.STRING_BYTES.domain(),
                NativeObjectKind.STRING_BYTES,
                slotId,
                1,
                0
        );
    }
}
