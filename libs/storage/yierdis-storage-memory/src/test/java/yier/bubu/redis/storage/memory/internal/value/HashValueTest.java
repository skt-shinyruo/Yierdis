package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class HashValueTest {
    @Test
    public void packedHashSupportsUpdateAndDeleteWithRepacking() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-test")) {
            HashValue hv = new HashValue(runtime);
            try {
                Assert.assertEquals(ValueEncoding.HASH_PACKED, hv.encoding());

                byte[] f1 = new byte[]{0, 'f', 1};
                byte[] v1 = new byte[]{'v'};
                byte[] f2 = new byte[]{'k'};
                byte[] v2 = new byte[]{'\n', 0, (byte) 0xFF};

                Assert.assertEquals(1, hv.hset(f1, v1));
                Assert.assertEquals(1, hv.hset(f2, v2));
                Assert.assertEquals(2, hv.size());

                Assert.assertArrayEquals(v1, hv.hget(f1));
                Assert.assertArrayEquals(v2, hv.hget(f2));

                byte[] v1Longer = new byte[]{'v', '0', '-', 'n', 'e', 'w'};
                Assert.assertEquals(0, hv.hset(f1, v1Longer));
                Assert.assertArrayEquals(v1Longer, hv.hget(f1));
                Assert.assertArrayEquals(v2, hv.hget(f2));

                Assert.assertEquals(1, hv.hdel(List.of(f2)));
                Assert.assertEquals(1, hv.size());
                Assert.assertNull(hv.hget(f2));
                Assert.assertArrayEquals(v1Longer, hv.hget(f1));

                Assert.assertEquals(1, hv.hdel(Arrays.asList(f1, f1)));
                Assert.assertEquals(0, hv.size());
                Assert.assertNull(hv.hget(f1));
            } finally {
                hv.close();
            }
        }
    }

    @Test
    public void hashConvertsToHashTableAfterTooManyFields() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-test")) {
            HashValue hv = new HashValue(runtime);
            try {
                int added = 0;
                for (int i = 0; i < 600; i++) {
                    byte[] f = ("f" + i).getBytes(StandardCharsets.US_ASCII);
                    byte[] v = ("v" + i).getBytes(StandardCharsets.US_ASCII);
                    added += hv.hset(f, v);
                }
                Assert.assertEquals(600, added);
                Assert.assertEquals(600, hv.size());
                Assert.assertEquals(ValueEncoding.HASH_HT, hv.encoding());
                Assert.assertArrayEquals("v0".getBytes(StandardCharsets.US_ASCII), hv.hget("f0".getBytes(StandardCharsets.US_ASCII)));
                Assert.assertArrayEquals("v599".getBytes(StandardCharsets.US_ASCII), hv.hget("f599".getBytes(StandardCharsets.US_ASCII)));
            } finally {
                hv.close();
            }
        }
    }
}
