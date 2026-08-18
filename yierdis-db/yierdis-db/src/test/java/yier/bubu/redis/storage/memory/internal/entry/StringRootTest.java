package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Arrays;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public class StringRootTest {
    @Test
    public void stringRootDoesNotMirrorEveryLiveHandle() {
        Assert.assertFalse(Arrays.stream(StringRoot.class.getDeclaredFields())
                .anyMatch(field -> Set.class.isAssignableFrom(field.getType())));
    }

    @Test
    public void stringRootStoresAndUpdatesNativeBytes() {
        try (TestBackend runtime = TestBackend.open("string-root")) {
            StableMemoryBackend backend = runtime.backend();
            StringRoot root = new StringRoot(backend);
            ValueHandle handle = root.store(new byte[] {'h', 'i'});
            try {
                Assert.assertEquals(ValueEncoding.STRING_RAW, root.encoding(handle));
                root.overwrite(handle, new byte[] {'o', 'k'});
                Assert.assertArrayEquals(new byte[] {'o', 'k'}, root.copy(handle));
                Assert.assertEquals(3, root.append(handle, new byte[] {'!'}));
                Assert.assertArrayEquals(new byte[] {'o', 'k', '!'}, root.copy(handle));
            } finally {
                root.release(handle);
            }
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void reallocKeepsCompleteHandleIdentity() {
        try (TestBackend runtime = TestBackend.open("string-root-realloc")) {
            StableMemoryBackend backend = runtime.backend();
            StringRoot root = new StringRoot(backend);
            ValueHandle handle = root.store(new byte[] {'a'});
            NativeHandle identity = handle.nativeHandle();
            try {
                root.append(handle, new byte[64 * 1024]);
                Assert.assertEquals(identity, handle.nativeHandle());
                Assert.assertEquals(64 * 1024 + 1, root.length(handle));
            } finally {
                root.release(handle);
            }
        }
    }

    @Test
    public void releasedStringHandleIsRejectedByTheBackend() {
        try (TestBackend runtime = TestBackend.open("string-root-stale")) {
            StableMemoryBackend backend = runtime.backend();
            StringRoot root = new StringRoot(backend);
            ValueHandle handle = root.store(new byte[] {'x'});
            root.release(handle);
            Assert.assertThrows(RuntimeException.class, () -> root.copy(handle));
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }
}
