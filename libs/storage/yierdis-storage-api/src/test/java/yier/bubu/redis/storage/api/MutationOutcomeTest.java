package yier.bubu.redis.storage.api;

import org.junit.Assert;
import org.junit.Test;

public class MutationOutcomeTest {
    @Test
    public void constantsAndFactoryRepresentChangedFacts() {
        Assert.assertFalse(MutationOutcome.NONE.changedAny());
        Assert.assertTrue(MutationOutcome.VALUE_CHANGED.valueChanged());
        Assert.assertFalse(MutationOutcome.VALUE_CHANGED.ttlChanged());
        Assert.assertSame(MutationOutcome.VALUE_AND_TTL_CHANGED, MutationOutcome.of(true, true));
    }

    @Test
    public void plusMergesValueAndTtlFacts() {
        MutationOutcome merged = MutationOutcome.VALUE_CHANGED.plus(MutationOutcome.TTL_CHANGED);

        Assert.assertTrue(merged.valueChanged());
        Assert.assertTrue(merged.ttlChanged());
        Assert.assertTrue(merged.changedAny());
    }
}
