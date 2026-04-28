package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.ops.MaxmemoryPolicy;

public class YierdisDbConstructionTest {
    @Test
    public void nullAndBlankMaxmemoryPoliciesDefaultToNoeviction() {
        assertConstructsWithPolicy(null);
        assertConstructsWithPolicy("");
        assertConstructsWithPolicy("   ");
    }

    @Test
    public void typedConfigDefaultsNullPolicyToNoeviction() {
        YierdisDbConfig config = YierdisDbConfig.create(0, null, 5, 5, 5);
        Assert.assertSame(MaxmemoryPolicy.NOEVICTION, config.maxmemoryPolicy);
    }

    @Test
    public void policyParsingNormalizesCaseAndUnderscore() {
        assertConstructsWithPolicy("ALLKEYS_RANDOM");
        assertConstructsWithPolicy("allkeys_LRU");
        assertConstructsWithPolicy("  NoEviction  ");
    }

    @Test
    public void typedConfigComputesLruEnabledFromCorePolicy() {
        YierdisDbConfig lru = YierdisDbConfig.create(1, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        Assert.assertTrue(lru.lruEnabled);

        YierdisDbConfig noLimit = YierdisDbConfig.create(0, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        Assert.assertFalse(noLimit.lruEnabled);

        YierdisDbConfig random = YierdisDbConfig.create(1, MaxmemoryPolicy.ALLKEYS_RANDOM, 5, 5, 5);
        Assert.assertFalse(random.lruEnabled);
    }

    @Test
    public void unknownPolicyStillThrowsIllegalArgumentException() {
        try {
            new YierdisDb((OffHeapAllocator) null, 0, "unknown-policy", 5, 5, 5);
            Assert.fail("unknown policy should fail construction");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("unknown maxmemory policy"));
        }
    }

    @Test
    public void invalidConstructionNumbersStillThrowIllegalArgumentException() {
        assertInvalid(-1, "noeviction", 5, 5, 5, "maxmemoryBytes");
        assertInvalid(0, "noeviction", 0, 5, 5, "maxmemorySamples");
        assertInvalid(0, "noeviction", 5, 0, 5, "evictionTimeLimitMillis");
        assertInvalid(0, "noeviction", 5, 5, 0, "expireCleanupTimeLimitMillis");
    }

    private static void assertConstructsWithPolicy(String policy) {
        YierdisDb db = new YierdisDb((OffHeapAllocator) null, 0, policy, 5, 5, 5);
        try {
            db.bindToCurrentThread();
        } finally {
            db.shutdown();
        }
    }

    private static void assertInvalid(
            long maxmemoryBytes,
            String policy,
            int samples,
            long evictionMillis,
            long expireMillis,
            String messagePart
    ) {
        try {
            new YierdisDb((OffHeapAllocator) null, maxmemoryBytes, policy, samples, evictionMillis, expireMillis);
            Assert.fail("invalid construction should fail: " + messagePart);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(messagePart));
        }
    }
}
