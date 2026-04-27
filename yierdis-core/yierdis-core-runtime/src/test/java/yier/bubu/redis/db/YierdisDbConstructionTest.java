package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

public class YierdisDbConstructionTest {
    @Test
    public void nullAndBlankMaxmemoryPoliciesDefaultToNoeviction() {
        assertConstructsWithPolicy(null);
        assertConstructsWithPolicy("");
        assertConstructsWithPolicy("   ");
    }

    @Test
    public void policyAdapterDefaultsNullAndBlankToNoeviction() {
        assertParsesPolicy(null, YierdisDb.MaxmemoryPolicy.NOEVICTION);
        assertParsesPolicy("", YierdisDb.MaxmemoryPolicy.NOEVICTION);
        assertParsesPolicy("   ", YierdisDb.MaxmemoryPolicy.NOEVICTION);
    }

    @Test
    public void policyParsingNormalizesCaseAndUnderscore() {
        assertConstructsWithPolicy("ALLKEYS_RANDOM");
        assertConstructsWithPolicy("allkeys_LRU");
        assertConstructsWithPolicy("  NoEviction  ");
    }

    @Test
    public void policyAdapterMapsExternalPoliciesToDbPolicies() {
        assertParsesPolicy("noeviction", YierdisDb.MaxmemoryPolicy.NOEVICTION);
        assertParsesPolicy("  NoEviction  ", YierdisDb.MaxmemoryPolicy.NOEVICTION);
        assertParsesPolicy("allkeys-random", YierdisDb.MaxmemoryPolicy.ALLKEYS_RANDOM);
        assertParsesPolicy("ALLKEYS_RANDOM", YierdisDb.MaxmemoryPolicy.ALLKEYS_RANDOM);
        assertParsesPolicy("allkeys-lru", YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU);
        assertParsesPolicy("allkeys_LRU", YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU);
    }

    @Test
    public void unknownPolicyStillThrowsIllegalArgumentException() {
        try {
            new YierdisDb((OffHeapAllocator) null, 0, "unknown-policy", 5, 5, 5);
            Assert.fail("unknown policy should fail construction");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("unsupported maxmemoryPolicy"));
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

    private static void assertParsesPolicy(String policy, YierdisDb.MaxmemoryPolicy expected) {
        Assert.assertSame(expected, YierdisDbMaxmemoryPolicies.parseOrDefault(policy));
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
