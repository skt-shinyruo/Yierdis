package yier.bubu.redis.storage.api;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class PreparedMutationContractTest {
    @Test
    public void previewAndValidationAreReadOnlyAndCommitIsExplicit() {
        AtomicInteger mutations = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        PreparedMutation<String> prepared = new PreparedMutation<>() {
            @Override
            public String preview() {
                return "result";
            }

            @Override
            public boolean isCurrent() {
                return true;
            }

            @Override
            public MutationOutcome commit() {
                mutations.incrementAndGet();
                return MutationOutcome.NONE;
            }

            @Override
            public void close() {
                closed.compareAndSet(false, true);
            }
        };

        Assert.assertEquals("result", prepared.preview());
        Assert.assertTrue(prepared.isCurrent());
        Assert.assertEquals(0, mutations.get());
        Assert.assertEquals(MutationOutcome.NONE, prepared.commit());
        Assert.assertEquals(1, mutations.get());
        prepared.close();
        prepared.close();
        Assert.assertTrue(closed.get());
    }
}
