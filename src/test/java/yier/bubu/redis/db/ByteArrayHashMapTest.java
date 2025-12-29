package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class ByteArrayHashMapTest {
    @Test
    public void putGetAndForEachWorkAcrossRehash() {
        ByteArrayHashMap<Integer> map = new ByteArrayHashMap<>(4);

        int n = 500;
        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            map.put(k, i);
        }
        Assert.assertEquals(n, map.size());

        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals((Integer) i, map.get(k));
        }

        HashSet<String> keys = new HashSet<>();
        map.forEach((k, v) -> keys.add(new String(k, StandardCharsets.UTF_8)));
        Assert.assertEquals(n, keys.size());
    }

    @Test
    public void removeKeyRemovesEntryEvenIfValueIsNull() {
        ByteArrayHashMap<byte[]> map = new ByteArrayHashMap<>();

        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        map.put(key, null);
        Assert.assertEquals(1, map.size());

        Assert.assertTrue(map.containsKey("k".getBytes(StandardCharsets.UTF_8)));
        Assert.assertTrue(map.removeKey("k".getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals(0, map.size());
        Assert.assertFalse(map.containsKey("k".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void tombstonesTriggerRebuildWithoutGrowing() throws Exception {
        ByteArrayHashMap<Integer> map = new ByteArrayHashMap<>(300);
        int beforeCap = capacity(map);

        int n = 300;
        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            map.put(k, i);
        }
        Assert.assertFalse(isRehashing(map));
        Assert.assertEquals(n, map.size());

        for (int i = 0; i < 200; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            Assert.assertTrue(map.removeKey(k));
        }
        boolean sawRehashing = isRehashing(map);

        byte[] keepKey = ("k" + 250).getBytes(StandardCharsets.UTF_8);
        while (isRehashing(map)) {
            map.get(keepKey);
        }

        Assert.assertEquals(100, map.size());
        int afterCap = capacity(map);
        Assert.assertTrue(sawRehashing || afterCap < beforeCap);
        Assert.assertTrue(afterCap <= beforeCap);
        Assert.assertEquals((Integer) 250, map.get(keepKey));
    }

    @Test
    public void sparseMapShrinksAfterDeletes() throws Exception {
        ByteArrayHashMap<Integer> map = new ByteArrayHashMap<>(512);
        int beforeCap = capacity(map);

        int n = 512;
        for (int i = 0; i < n; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            map.put(k, i);
        }
        Assert.assertEquals(n, map.size());

        for (int i = 0; i < 412; i++) {
            byte[] k = ("k" + i).getBytes(StandardCharsets.UTF_8);
            map.removeKey(k);
        }

        byte[] keepKey = ("k" + 500).getBytes(StandardCharsets.UTF_8);
        while (isRehashing(map)) {
            map.get(keepKey);
        }

        Assert.assertEquals(100, map.size());
        int afterCap = capacity(map);
        Assert.assertTrue(afterCap < beforeCap);
        Assert.assertEquals((Integer) 500, map.get(keepKey));
    }

    private static boolean isRehashing(ByteArrayHashMap<?> map) throws Exception {
        Field f = ByteArrayHashMap.class.getDeclaredField("keys1");
        f.setAccessible(true);
        return f.get(map) != null;
    }

    private static int capacity(ByteArrayHashMap<?> map) throws Exception {
        Field f = ByteArrayHashMap.class.getDeclaredField("keys0");
        f.setAccessible(true);
        return ((byte[][]) f.get(map)).length;
    }
}
