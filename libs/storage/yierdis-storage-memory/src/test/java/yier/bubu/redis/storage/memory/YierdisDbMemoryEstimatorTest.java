package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.DbMemoryConstants;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class YierdisDbMemoryEstimatorTest {
    @Test
    public void estimatesHeapStringEntryBytesIncludingHeapKey() {
        YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator(false, null);
        KeyHandle key = KeyHandle.forHeap(b("abc"), 1);
        YierdisObject object = YierdisObject.newString(null, b("hello"));

        long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 3L + 5L;

        Assert.assertEquals(expected, estimator.estimateEntryBytes(key, object));
    }

    @Test
    public void estimatesHeapStringEntryBytesExcludingOffHeapKey() {
        YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator(true, null);
        KeyHandle key = KeyHandle.forHeap(b("abc"), 1);
        YierdisObject object = YierdisObject.newString(null, b("hello"));

        long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 5L;

        Assert.assertEquals(expected, estimator.estimateEntryBytes(key, object));
    }

    @Test
    public void estimatesIntegerEncodedStringPayloadAsLongBytes() {
        YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator(false, null);
        KeyHandle key = KeyHandle.forHeap(b("n"), 1);
        YierdisObject object = YierdisObject.newStringInt(42L);

        long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + Long.BYTES;

        Assert.assertEquals(expected, estimator.estimateEntryBytes(key, object));
    }

    @Test
    public void estimatesWriteUpperBoundsAndByteSums() {
        Assert.assertEquals(
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 3L + 5L,
                YierdisDbMemoryEstimator.estimateStringWriteUpperBound(3, 5)
        );
        Assert.assertEquals(6L, YierdisDbMemoryEstimator.sumByteLengths(List.of(b("a"), b("bc"), b("def"))));
        Assert.assertEquals(4L, YierdisDbMemoryEstimator.sumZSetMemberByteLengths(List.of(b("1"), b("aa"), b("2"), b("bb"))));
    }

    @Test
    public void estimatesSetAndZSetCreationUpperBounds() {
        long setExpected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + 3L + (2L * 32L);
        long zsetExpected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + 4L + (2L * 96L);

        Assert.assertEquals(setExpected, YierdisDbMemoryEstimator.estimateSetWriteUpperBound(1, List.of(b("a"), b("bc"))));
        Assert.assertEquals(zsetExpected, YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(1, List.of(b("1"), b("aa"), b("2"), b("bb"))));
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
