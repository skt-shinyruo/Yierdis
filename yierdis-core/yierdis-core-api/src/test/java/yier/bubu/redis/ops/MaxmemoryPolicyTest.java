package yier.bubu.redis.ops;

import org.junit.Assert;
import org.junit.Test;

public class MaxmemoryPolicyTest {
    @Test
    public void parse_shouldParseKnownPolicies() {
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, MaxmemoryPolicy.parse("noeviction"));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, MaxmemoryPolicy.parse("allkeys-random"));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, MaxmemoryPolicy.parse("allkeys-lru"));
    }

    @Test
    public void parse_shouldNormalizeTrimCaseAndUnderscore() {
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, MaxmemoryPolicy.parse("  NoEviction  "));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, MaxmemoryPolicy.parse("ALLKEYS_RANDOM"));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, MaxmemoryPolicy.parse("allkeys_LRU"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_shouldThrowOnUnknownPolicies() {
        MaxmemoryPolicy.parse("unknown-policy");
    }
}
