package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class YierdisDictTest {
    @Test
    public void computeGetAndForEachWorkAcrossRehash() {
        YierdisDict<Integer> dict = new YierdisDict<>();

        int n = 200;
        for (int i = 0; i < n; i++) {
            ByteArrayKey k = new ByteArrayKey(("k" + i).getBytes(StandardCharsets.UTF_8));
            int v = i;
            dict.compute(k, (key, old) -> v);
        }
        Assert.assertEquals(n, dict.size());

        for (int i = 0; i < n; i++) {
            ByteArrayKey k = new ByteArrayKey(("k" + i).getBytes(StandardCharsets.UTF_8));
            Assert.assertEquals((Integer) i, dict.get(k));
        }

        HashSet<String> keys = new HashSet<>();
        dict.forEach((k, v) -> keys.add(new String(k.bytes(), StandardCharsets.UTF_8)));
        Assert.assertEquals(n, keys.size());
    }

    @Test
    public void removeHonorsExpectedValueIdentity() {
        YierdisDict<Object> dict = new YierdisDict<>();

        ByteArrayKey key = new ByteArrayKey("k".getBytes(StandardCharsets.UTF_8));
        Object v1 = new Object();
        dict.compute(key, (k, old) -> v1);

        Assert.assertFalse(dict.remove(key, new Object()));
        Assert.assertTrue(dict.remove(key, v1));
        Assert.assertNull(dict.get(key));
    }
}

