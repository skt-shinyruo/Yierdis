package yier.bubu.redis.storage.memory.internal.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SipHashIntIndexTest {
    private static final HashSeed SEED = HashSeed.random();

    @Test
    public void addIfAbsentDeduplicatesByContent() {
        List<byte[]> keys = new ArrayList<>();
        SipHashIntIndex index = new SipHashIntIndex(keys::get, 4, SEED, "capacity exceeded");
        keys.add(new byte[]{1, 2});
        Assert.assertTrue(index.addIfAbsent(0));
        keys.add(new byte[]{1, 2});
        Assert.assertFalse(index.addIfAbsent(1));
        Assert.assertEquals(0, index.find(new byte[]{1, 2}));
    }

    @Test
    public void findReturnsMinusOneForMissingKey() {
        List<byte[]> keys = new ArrayList<>(List.of(new byte[]{7}));
        SipHashIntIndex index = new SipHashIntIndex(keys::get, 4, SEED, "capacity exceeded");
        index.add(0);
        Assert.assertEquals(-1, index.find(new byte[]{8}));
    }

    @Test
    public void clearRemovesAllEntries() {
        List<byte[]> keys = new ArrayList<>(List.of(new byte[]{1}, new byte[]{2}));
        SipHashIntIndex index = new SipHashIntIndex(keys::get, 4, SEED, "capacity exceeded");
        Assert.assertTrue(index.addIfAbsent(0));
        Assert.assertTrue(index.addIfAbsent(1));
        index.clear();
        Assert.assertEquals(-1, index.find(new byte[]{1}));
        Assert.assertTrue(index.addIfAbsent(0));
        Assert.assertEquals(0, index.find(new byte[]{1}));
    }

    @Test
    public void findsAllKeysAcrossPowerOfTwoGrowth() {
        int count = 10_000;
        List<byte[]> keys = new ArrayList<>(count);
        SipHashIntIndex index = new SipHashIntIndex(keys::get, count, SEED, "capacity exceeded");
        for (int i = 0; i < count; i++) {
            keys.add(new byte[]{(byte) (i >>> 8), (byte) i});
            Assert.assertTrue(index.addIfAbsent(i));
        }
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(i, index.find(new byte[]{(byte) (i >>> 8), (byte) i}));
        }
    }

    @Test
    public void rejectsExpectedEntriesAboveCapacityLimit() {
        List<byte[]> keys = new ArrayList<>();
        IllegalArgumentException failure = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> new SipHashIntIndex(keys::get, (1 << 29) + 1L, SEED, "capacity exceeded")
        );
        Assert.assertEquals("capacity exceeded", failure.getMessage());
    }
}
