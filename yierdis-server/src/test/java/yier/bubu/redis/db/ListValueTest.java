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
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

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

    private static int elementBytesSoTwoFitThreeDoNot(int maxNodeBytes) {
        // Finds a stable "large element" size such that:
        // - 2 encoded entries fit within a node
        // - 3 encoded entries do not fit within a node
        //
        // This keeps the test robust to small encoding changes (e.g., varint header sizes).
        for (int candidate = Math.max(65, maxNodeBytes); candidate >= 65; candidate--) {
            int entryBytes = encodedListpackEntryBytes(candidate);
            if ((long) entryBytes * 2 <= maxNodeBytes && (long) entryBytes * 3 > maxNodeBytes) {
                return candidate;
            }
        }
        throw new AssertionError("unable to find element size for node bytes=" + maxNodeBytes);
    }

    private static int encodedListpackEntryBytes(int rawLen) {
        int headerValue = rawLen + 1;
        return varIntSize(headerValue) + rawLen;
    }

    private static int varIntSize(int value) {
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
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
