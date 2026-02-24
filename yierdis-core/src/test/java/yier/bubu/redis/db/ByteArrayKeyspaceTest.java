package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class ByteArrayKeyspaceTest {
    @Test
    public void sliceLookupFindsExistingKeysAndReturnsCanonicalKey() {
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>();

        byte[] storedKey = new byte[]{0, (byte) 0xFF, 'k'};
        keyspace.compute(storedKey, (k, old) -> 123);

        byte[] other = new byte[]{0, (byte) 0xFF, 'k', 'x'};
        BytesView view = new TestBytesView(other, 0, 3);

        Assert.assertSame(storedKey, keyspace.canonicalKey(view));
        Assert.assertEquals((Integer) 123, keyspace.get(view));

        BytesView miss = new TestBytesView(new byte[]{0, (byte) 0xFE, 'k'}, 0, 3);
        Assert.assertNull(keyspace.canonicalKey(miss));
        Assert.assertNull(keyspace.get(miss));
    }

    @Test
    public void computeGetAndForEachWorkAcrossRehash() {
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>(4);

        int n = 200;
        boolean sawRehashing = false;
        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            int v = i;
            keyspace.compute(k, (key, old) -> v);
            sawRehashing |= keyspace.isRehashing();
        }
        Assert.assertTrue("rehashing should start for a small initial table", sawRehashing);
        Assert.assertEquals(n, keyspace.size());

        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals((Integer) i, keyspace.get(k));
        }

        HashSet<String> keys = new HashSet<>();
        keyspace.forEach((k, v) -> keys.add(new String(k, StandardCharsets.UTF_8)));
        Assert.assertEquals(n, keys.size());
    }

    @Test
    public void removeHonorsExpectedValueIdentity() {
        ByteArrayKeyspace<Object> keyspace = new ByteArrayKeyspace<>();

        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        Object v1 = new Object();
        keyspace.compute(key, (k, old) -> v1);

        Assert.assertFalse(keyspace.remove("k".getBytes(StandardCharsets.UTF_8), new Object()));
        Assert.assertTrue(keyspace.remove("k".getBytes(StandardCharsets.UTF_8), v1));
        Assert.assertNull(keyspace.get("k".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void computeIfPresentDoesNotInsertAndCanRemove() {
        ByteArrayKeyspace<String> keyspace = new ByteArrayKeyspace<>();

        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        Assert.assertNull(keyspace.computeIfPresent(key, (k, old) -> "v"));
        Assert.assertNull(keyspace.get("k".getBytes(StandardCharsets.UTF_8)));

        keyspace.compute(key, (k, old) -> "v1");
        Assert.assertEquals("v12",
                keyspace.computeIfPresent("k".getBytes(StandardCharsets.UTF_8), (k, old) -> old + "2"));
        Assert.assertEquals("v12", keyspace.get("k".getBytes(StandardCharsets.UTF_8)));

        Assert.assertNull(keyspace.computeIfPresent("k".getBytes(StandardCharsets.UTF_8), (k, old) -> null));
        Assert.assertNull(keyspace.get("k".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void removedKeysCanBeReinserted() {
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>(4);

        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        Integer v1 = keyspace.compute(key, (k, old) -> 1);
        Assert.assertTrue(keyspace.remove("k".getBytes(StandardCharsets.UTF_8), v1));
        Assert.assertNull(keyspace.get("k".getBytes(StandardCharsets.UTF_8)));

        keyspace.compute("k".getBytes(StandardCharsets.UTF_8), (k, old) -> 2);
        Assert.assertEquals((Integer) 2, keyspace.get("k".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void tombstonesTriggerRebuildWithoutGrowing() throws Exception {
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>(300);
        int beforeCap = capacity(keyspace);

        int n = 300;
        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            int v = i;
            keyspace.compute(k, (key, old) -> v);
        }
        Assert.assertFalse(keyspace.isRehashing());
        Assert.assertEquals(n, keyspace.size());

        for (int i = 0; i < 200; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            keyspace.computeIfPresent(k, (key, old) -> null);
        }
        boolean sawRehashing = keyspace.isRehashing();

        byte[] keepKey = ("k" + 250).getBytes(StandardCharsets.UTF_8);
        while (keyspace.isRehashing()) {
            keyspace.get(keepKey);
        }

        Assert.assertEquals(100, keyspace.size());
        int afterCap = capacity(keyspace);
        Assert.assertTrue(sawRehashing || afterCap < beforeCap);
        Assert.assertTrue(afterCap <= beforeCap);
        Assert.assertEquals((Integer) 250, keyspace.get(keepKey));
    }

    @Test
    public void sparseKeyspaceShrinksAfterDeletes() throws Exception {
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>(512);
        int beforeCap = capacity(keyspace);

        int n = 512;
        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            int v = i;
            keyspace.compute(k, (key, old) -> v);
        }
        Assert.assertEquals(n, keyspace.size());

        for (int i = 0; i < 412; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            keyspace.computeIfPresent(k, (key, old) -> null);
        }

        byte[] keepKey = ("k" + 500).getBytes(StandardCharsets.UTF_8);
        while (keyspace.isRehashing()) {
            keyspace.get(keepKey);
        }

        Assert.assertEquals(100, keyspace.size());
        int afterCap = capacity(keyspace);
        Assert.assertTrue(afterCap < beforeCap);
        Assert.assertEquals((Integer) 500, keyspace.get(keepKey));
    }

    private static int capacity(ByteArrayKeyspace<?> keyspace) throws Exception {
        Field f = ByteArrayKeyspace.class.getDeclaredField("keys0");
        f.setAccessible(true);
        return ((byte[][]) f.get(keyspace)).length;
    }

    private static final class TestBytesView implements yier.bubu.redis.bytes.BytesView {
        private final byte[] data;
        private final int off;
        private final int len;

        private TestBytesView(byte[] data, int off, int len) {
            this.data = data;
            this.off = off;
            this.len = len;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public byte getByte(int index) {
            if (index < 0 || index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return data[off + index];
        }
    }
}
