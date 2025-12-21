package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class ByteArrayKeyspaceTest {
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
}

