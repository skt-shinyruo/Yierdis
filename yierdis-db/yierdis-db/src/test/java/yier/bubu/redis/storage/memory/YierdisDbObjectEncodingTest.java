package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.nio.charset.StandardCharsets;

public class YierdisDbObjectEncodingTest {
    @Test
    public void objectEncodingReadsNativeEntryEncoding() {
        try (TestBackend runtime = TestBackend.open("introspection-encoding")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                byte[] key = bytes("encoding-key");
                db.strings().setString(key, bytes("value"), SetMode.NORMAL, null);

                Assert.assertEquals("embstr", db.objectEncoding(view(key)));
                Assert.assertNull(db.objectEncoding(view(bytes("missing"))));
            } finally {
                db.shutdown();
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesView view(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }
}
