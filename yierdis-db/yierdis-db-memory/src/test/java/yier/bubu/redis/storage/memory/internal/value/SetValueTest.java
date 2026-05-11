package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class SetValueTest {
    @Test
    public void ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("set-test")) {
            SetValue sv = new SetValue(runtime);
            try {
                Assert.assertEquals(ValueEncoding.SET_INTSET, sv.encoding());

                Assert.assertEquals(2, sv.addAll(List.of(b("1"), b("2"))));
                Assert.assertEquals(2, sv.size());
                Assert.assertTrue(sv.contains(b("1")));
                Assert.assertTrue(sv.estimatedBytes() > 0);

                Assert.assertEquals(1, sv.addAll(List.of(b("alpha"))));
                Assert.assertEquals(ValueEncoding.SET_HT, sv.encoding());
                Assert.assertEquals(3, sv.size());
                Assert.assertTrue(sv.contains(b("alpha")));
                Assert.assertEquals(1, sv.removeAll(List.of(b("2"))));
                Assert.assertFalse(sv.contains(b("2")));
            } finally {
                sv.close();
            }
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
