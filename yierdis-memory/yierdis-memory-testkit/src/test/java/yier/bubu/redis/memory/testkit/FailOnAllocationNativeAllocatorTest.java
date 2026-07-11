package yier.bubu.redis.memory.testkit;

import java.lang.reflect.Proxy;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class FailOnAllocationNativeAllocatorTest {
    @Test
    public void failsExactlyTheConfiguredAllocationAndCanBeReset() {
        NativeAllocator delegate = recordingAllocator();
        FailOnAllocationNativeAllocator allocator = new FailOnAllocationNativeAllocator(delegate);
        allocator.failOnAllocation(2);

        allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
        Assert.assertThrows(
                NativeCapacityExceededException.class,
                () -> allocator.allocate(NativeObjectKind.STRING_BYTES, 1)
        );
        Assert.assertEquals(2L, allocator.allocationAttempts());

        allocator.disableFailures();
        allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
        Assert.assertEquals(3L, allocator.allocationAttempts());
    }

    private static NativeAllocator recordingAllocator() {
        return (NativeAllocator) Proxy.newProxyInstance(
                NativeAllocator.class.getClassLoader(),
                new Class<?>[] {NativeAllocator.class},
                (proxy, method, args) -> null
        );
    }
}
