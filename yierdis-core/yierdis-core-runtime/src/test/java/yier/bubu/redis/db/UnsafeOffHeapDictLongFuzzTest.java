package yier.bubu.redis.db;

import io.netty.util.internal.PlatformDependent;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapDictLong;
import yier.bubu.redis.db.memory.unsafe.YierdisUnsafeOffHeapAllocator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class UnsafeOffHeapDictLongFuzzTest {
    @Test
    public void randomOpsPreserveMapInvariants() {
        Random random = new Random(12345);
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        try {
            YierdisUnsafeOffHeapDictLong dict = new YierdisUnsafeOffHeapDictLong(allocator);
            try {
                Map<String, Long> ref = new HashMap<>();
                ArrayList<String> liveKeys = new ArrayList<>();

                int ops = 20_000;
                for (int i = 0; i < ops; i++) {
                    int op = random.nextInt(100);
                    if (op < 55) {
                        doPut(random, dict, ref, liveKeys);
                    } else if (op < 80) {
                        doGet(random, dict, ref, liveKeys);
                    } else {
                        doRemove(random, dict, ref, liveKeys);
                    }

                    Assert.assertEquals(ref.size(), dict.size());
                    if ((i & 1023) == 0) {
                        assertAllPresent(dict, ref);
                    }
                }

                assertAllPresent(dict, ref);

                Map<String, Long> seen = new HashMap<>();
                dict.forEach((keyPtr, keyLen, value) -> {
                    byte[] key = new byte[keyLen];
                    PlatformDependent.copyMemory(keyPtr, key, 0, keyLen);
                    String s = new String(key, StandardCharsets.ISO_8859_1);
                    seen.put(s, value);
                });
                Assert.assertEquals(ref, seen);
            } finally {
                dict.close();
            }
        } finally {
            allocator.close();
        }
    }

    private static void doPut(
            Random random,
            YierdisUnsafeOffHeapDictLong dict,
            Map<String, Long> ref,
            ArrayList<String> liveKeys
    ) {
        String keyStr;
        byte[] keyBytes;
        boolean updateExisting = !liveKeys.isEmpty() && random.nextInt(100) < 40;
        if (updateExisting) {
            keyStr = liveKeys.get(random.nextInt(liveKeys.size()));
            keyBytes = keyStr.getBytes(StandardCharsets.ISO_8859_1);
        } else {
            while (true) {
                keyBytes = randomKeyBytes(random);
                keyStr = new String(keyBytes, StandardCharsets.ISO_8859_1);
                if (!ref.containsKey(keyStr)) {
                    break;
                }
            }
        }

        long value = randomNonZeroPositiveLong(random);

        Long oldRef = ref.put(keyStr, value);
        long old = dict.put(keyBytes, value);

        Assert.assertEquals(oldRef == null ? 0L : oldRef.longValue(), old);
        if (oldRef == null) {
            liveKeys.add(keyStr);
        }
    }

    private static void doGet(
            Random random,
            YierdisUnsafeOffHeapDictLong dict,
            Map<String, Long> ref,
            ArrayList<String> liveKeys
    ) {
        byte[] keyBytes;
        String keyStr;
        if (!liveKeys.isEmpty() && random.nextInt(100) < 70) {
            keyStr = liveKeys.get(random.nextInt(liveKeys.size()));
            keyBytes = keyStr.getBytes(StandardCharsets.ISO_8859_1);
        } else {
            keyBytes = randomKeyBytes(random);
            keyStr = new String(keyBytes, StandardCharsets.ISO_8859_1);
        }

        long got = dict.get(keyBytes);
        Long expected = ref.get(keyStr);
        Assert.assertEquals(expected == null ? 0L : expected.longValue(), got);
    }

    private static void doRemove(
            Random random,
            YierdisUnsafeOffHeapDictLong dict,
            Map<String, Long> ref,
            ArrayList<String> liveKeys
    ) {
        byte[] keyBytes;
        String keyStr;
        boolean existing = !liveKeys.isEmpty() && random.nextInt(100) < 70;
        if (existing) {
            keyStr = liveKeys.get(random.nextInt(liveKeys.size()));
            keyBytes = keyStr.getBytes(StandardCharsets.ISO_8859_1);
        } else {
            keyBytes = randomKeyBytes(random);
            keyStr = new String(keyBytes, StandardCharsets.ISO_8859_1);
        }

        Long expected = ref.remove(keyStr);
        long removed;
        if (expected != null && random.nextBoolean()) {
            YierdisUnsafeOffHeapDictLong.KeyHandle handle = dict.keyHandle(keyBytes);
            Assert.assertNotNull(handle);
            removed = dict.removeByPtr(handle.keyPtr, handle.keyLen, handle.hash);
        } else {
            removed = dict.remove(keyBytes);
        }
        Assert.assertEquals(expected == null ? 0L : expected.longValue(), removed);

        if (expected != null) {
            liveKeys.remove(keyStr);
        }
    }

    private static void assertAllPresent(YierdisUnsafeOffHeapDictLong dict, Map<String, Long> ref) {
        for (Map.Entry<String, Long> entry : ref.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.ISO_8859_1);
            Assert.assertEquals(entry.getValue().longValue(), dict.get(key));
        }
    }

    private static byte[] randomKeyBytes(Random random) {
        int len = 1 + random.nextInt(32);
        byte[] out = new byte[len];
        random.nextBytes(out);
        return out;
    }

    private static long randomNonZeroPositiveLong(Random random) {
        long v = random.nextLong() & Long.MAX_VALUE;
        return v == 0L ? 1L : v;
    }
}

