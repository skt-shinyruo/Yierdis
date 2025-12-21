package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;

public class ListValueTest {
    @Test
    public void packedListPreservesNullVsEmptyElements() {
        ListValue lv = new ListValue();
        lv.rpushAll(Arrays.asList(null, new byte[0], "a".getBytes(StandardCharsets.US_ASCII)));

        Assert.assertEquals(3, lv.size());

        List<byte[]> all = lv.range(0, -1);
        Assert.assertEquals(3, all.size());
        Assert.assertNull(all.get(0));
        Assert.assertNotNull(all.get(1));
        Assert.assertEquals(0, all.get(1).length);
        Assert.assertArrayEquals("a".getBytes(StandardCharsets.US_ASCII), all.get(2));

        List<byte[]> popped = lv.lpop(1);
        Assert.assertEquals(1, popped.size());
        Assert.assertNull(popped.get(0));
        Assert.assertEquals(2, lv.size());
    }

    @Test
    public void quicklistSplitsByBytesAndMerges() throws Exception {
        int maxBytes = (int) getStaticInt(ListValue.class, "QUICKLIST_NODE_MAX_BYTES");
        int elementBytes = Math.max(65, maxBytes / 2); // 2 elements can fit, 3 cannot (for typical maxBytes)

        ListValue lv = new ListValue();
        List<byte[]> in = new ArrayList<>();
        in.add(new byte[elementBytes]);
        in.add(new byte[elementBytes]);
        in.add(new byte[elementBytes]);

        lv.rpushAll(in);
        Assert.assertEquals(3, lv.size());
        Assert.assertEquals(2, quicklistNodeCount(lv));

        lv.lpop(1);
        Assert.assertEquals(2, lv.size());
        Assert.assertEquals(1, quicklistNodeCount(lv));
    }

    private static int quicklistNodeCount(ListValue lv) throws Exception {
        Field f = ListValue.class.getDeclaredField("quicklist");
        f.setAccessible(true);
        Object ql = f.get(lv);
        Assert.assertNotNull("Expected quicklist mode", ql);
        @SuppressWarnings("unchecked")
        ArrayDeque<?> deque = (ArrayDeque<?>) ql;
        return deque.size();
    }

    private static Object getStaticInt(Class<?> clazz, String fieldName) throws Exception {
        Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(null);
    }
}
