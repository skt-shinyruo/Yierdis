package yier.bubu.redis.execution.api;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.Assert;
import org.junit.Test;

public class TransactionStateContractTest {
    @Test
    public void transactionStateHasNoDefaultOwnershipOrAbortMethods() throws Exception {
        for (String name : new String[]{
                "aborted", "markAborted", "tryEnqueue", "forEachQueued",
                "drain", "discard", "close"
        }) {
            Method method = java.util.Arrays.stream(TransactionState.class.getMethods())
                    .filter(candidate -> candidate.getName().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("missing method: " + name));
            Assert.assertFalse("default method remains: " + name, method.isDefault());
            Assert.assertTrue(Modifier.isPublic(method.getModifiers()));
        }
    }
}
