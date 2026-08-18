package yier.bubu.redis.storage.memory.internal.value;

import java.lang.reflect.Proxy;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;

public class NativeHandleSetTest {
    @Test
    public void keepsCollisionHandlesDistinctThroughTheirFullLifecycle() {
        TestOwner leftOwner = new TestOwner();
        TestOwner rightOwner = new TestOwner();
        HeapStableMemoryBackend leftBackend = new HeapStableMemoryBackend("left", 8, leftOwner);
        HeapStableMemoryBackend rightBackend = new HeapStableMemoryBackend("right", 8, rightOwner);
        leftBackend.bindToCurrentThread();
        rightBackend.bindToCurrentThread();

        NativeHandle left = leftBackend.allocate(NativeObjectKind.STRING_BYTES, 1);
        NativeHandle right = rightBackend.allocate(NativeObjectKind.STRING_BYTES, 1);
        Assert.assertEquals(left.localRaw(), right.localRaw());
        Assert.assertNotEquals(left.allocatorId(), right.allocatorId());

        NativeHandleSet handles = new NativeHandleSet(2);
        Assert.assertTrue(handles.add(left));
        Assert.assertTrue(handles.add(right));
        Assert.assertFalse(handles.add(left));
        Assert.assertEquals(2, handles.size());
        Assert.assertTrue(handles.contains(left));
        Assert.assertTrue(handles.contains(right));

        StableMemoryBackend routedBackends = route(leftBackend, rightBackend);
        handles.pinAll(routedBackends);
        handles.unpinAll(routedBackends);
        handles.freeAll(routedBackends);

        leftBackend.close();
        rightBackend.close();
    }

    @Test
    public void emptySetHasAnExplicitCapacityBoundary() {
        NativeHandleSet handles = new NativeHandleSet(0);

        Assert.assertEquals(0, handles.size());
        Assert.assertFalse(handles.contains(new NativeHandle(1L, 1L)));
        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> handles.add(new NativeHandle(1L, 1L))
        );
        Assert.assertEquals("native handle set has zero capacity", failure.getMessage());
    }

    private static StableMemoryBackend route(
            StableMemoryBackend left,
            StableMemoryBackend right
    ) {
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[] {StableMemoryBackend.class},
                (proxy, method, arguments) -> {
                    NativeHandle handle = arguments == null || arguments.length == 0
                            ? null
                            : (NativeHandle) arguments[0];
                    if (handle == null) {
                        throw new UnsupportedOperationException(method.getName());
                    }
                    StableMemoryBackend backend = handle.allocatorId() == left.allocatorId() ? left : right;
                    return switch (method.getName()) {
                        case "pin" -> {
                            backend.pin(handle);
                            yield null;
                        }
                        case "unpin" -> {
                            backend.unpin(handle);
                            yield null;
                        }
                        case "free" -> {
                            backend.free(handle);
                            yield null;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                }
        );
    }

    private static final class TestOwner implements MemoryOwner {
        private Thread owner;

        @Override
        public void bindToCurrentThread() {
            Thread current = Thread.currentThread();
            if (owner == null) {
                owner = current;
                return;
            }
            if (owner != current) {
                throw new IllegalStateException("owner is bound to another thread");
            }
        }

        @Override
        public void checkCurrentThread() {
            if (owner != Thread.currentThread()) {
                throw new IllegalStateException("access is outside the owner thread");
            }
        }

        @Override
        public void checkCurrentThreadForShutdown() {
            if (owner != null && owner != Thread.currentThread()) {
                throw new IllegalStateException("shutdown is outside the owner thread");
            }
        }
    }
}
