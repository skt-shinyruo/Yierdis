package yier.bubu.redis.protocol;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSource;

import java.nio.charset.StandardCharsets;

public class RespWireSkipperTest {
    @Test
    public void skipsSimpleLineTypes() {
        assertSkipOk("+OK\r\n");
        assertSkipOk("-ERR boom\r\n");
        assertSkipOk(",3.14\r\n");
        assertSkipOk("(123\r\n");
    }

    @Test
    public void skipsNullAndBoolean() {
        assertSkipOk("_\r\n");
        assertSkipOk("#t\r\n");
        assertSkipOk("#f\r\n");
    }

    @Test
    public void skipsBulkAndStreamedBulk() {
        assertSkipOk("$0\r\n\r\n");
        assertSkipOk("$5\r\nhello\r\n");
        assertSkipOk("$-1\r\n");
        assertSkipOk("$?\r\n;5\r\nhello\r\n;0\r\n");
    }

    @Test
    public void skipsStreamedAggregates() {
        assertSkipOk("*?\r\n:1\r\n:2\r\n.\r\n");
        assertSkipOk("%?\r\n+key\r\n+value\r\n.\r\n");
        assertSkipOk("~?\r\n+v\r\n.\r\n");
    }

    @Test
    public void skipsAttributesWrapperAsSingleValue() {
        String payload = "|1\r\n+meta\r\n:123\r\n+OK\r\n";
        assertSkipOk(payload);
    }

    @Test
    public void skipsChainedAttributesWrapperAsSingleValue() {
        String payload = "|0\r\n|0\r\n+OK\r\n";
        assertSkipOk(payload);
    }

    @Test
    public void skipsAttributeMapOnlyForRequestDecoder() {
        byte[] bytes = ascii("|1\r\n+meta\r\n:1\r\n*1\r\n+PING\r\n");
        BytesSource src = new HeapBytesSource(bytes);
        int endMap = RespWireSkipper.trySkipAttributeMapOnly(src,
                0,
                bytes.length,
                RespLimits.DEFAULT_MAX_BULK_BYTES,
                RespLimits.DEFAULT_MAX_ARRAY_LEN,
                RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                RespLimits.DEFAULT_MAX_LINE_BYTES
        );
        Assert.assertTrue(endMap > 0);
        // attribute map ends before the actual command array; next byte should be '*'
        Assert.assertEquals((byte) '*', src.getByte(endMap));
    }

    @Test
    public void incompleteDataReturnsMinusOneInsteadOfThrowing() {
        byte[] bytes = ascii("$5\r\nhe");
        int end = RespWireSkipper.trySkipOne(new HeapBytesSource(bytes),
                0,
                bytes.length,
                RespLimits.DEFAULT_MAX_BULK_BYTES,
                RespLimits.DEFAULT_MAX_ARRAY_LEN,
                RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                RespLimits.DEFAULT_MAX_LINE_BYTES
        );
        Assert.assertEquals(-1, end);
    }

    @Test
    public void streamedMapMissingValueBeforeEndMarkerIsProtocolError() {
        try {
            assertSkipOk("%?\r\n+key\r\n.\r\n");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("map value"));
        }
    }

    @Test
    public void strictSkipRejectsNullForNonDollarBulkTypes() {
        // reply/framing skipper: allowNullForNonDollarBulk=true (historical compatibility)
        assertSkipOk("=-1\r\n");
        assertSkipOk("!-1\r\n");

        // request/strict skipper: allowNullForNonDollarBulk=false
        assertSkipStrictProtocolError("=-1\r\n", "invalid bulk length");
        assertSkipStrictProtocolError("!-1\r\n", "invalid bulk length");
    }

    @Test
    public void respectsMaxLineBytes() {
        byte[] bytes = ascii("+abcdefg");
        try {
            RespWireSkipper.trySkipOne(
                    new HeapBytesSource(bytes),
                    0,
                    bytes.length,
                    RespLimits.DEFAULT_MAX_BULK_BYTES,
                    RespLimits.DEFAULT_MAX_ARRAY_LEN,
                    RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                    4
            );
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("line too long"));
        }
    }

    @Test
    public void respectsMaxNestingDepth() {
        String nested = "*1\r\n*1\r\n:0\r\n";
        byte[] bytes = ascii(nested);
        try {
            RespWireSkipper.trySkipOne(
                    new HeapBytesSource(bytes),
                    0,
                    bytes.length,
                    RespLimits.DEFAULT_MAX_BULK_BYTES,
                    RespLimits.DEFAULT_MAX_ARRAY_LEN,
                    1,
                    RespLimits.DEFAULT_MAX_LINE_BYTES
            );
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("nested arrays too deep"));
        }
    }

    @Test
    public void respectsMaxArrayLen() {
        String payload = "*2\r\n:1\r\n:2\r\n";
        byte[] bytes = ascii(payload);
        try {
            RespWireSkipper.trySkipOne(
                    new HeapBytesSource(bytes),
                    0,
                    bytes.length,
                    RespLimits.DEFAULT_MAX_BULK_BYTES,
                    1,
                    RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                    RespLimits.DEFAULT_MAX_LINE_BYTES
            );
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("array length too large"));
        }
    }

    private static void assertSkipOk(String payload) {
        byte[] bytes = ascii(payload);
        int end = RespWireSkipper.trySkipOne(new HeapBytesSource(bytes),
                0,
                bytes.length,
                RespLimits.DEFAULT_MAX_BULK_BYTES,
                RespLimits.DEFAULT_MAX_ARRAY_LEN,
                RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                RespLimits.DEFAULT_MAX_LINE_BYTES
        );
        Assert.assertEquals("expected skip to consume full payload", bytes.length, end);
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static void assertSkipStrictProtocolError(String payload, String messageContains) {
        byte[] bytes = ascii(payload);
        try {
            RespWireSkipper.trySkipOneStrict(
                    new HeapBytesSource(bytes),
                    0,
                    bytes.length,
                    RespLimits.DEFAULT_MAX_BULK_BYTES,
                    RespLimits.DEFAULT_MAX_ARRAY_LEN,
                    RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                    RespLimits.DEFAULT_MAX_LINE_BYTES
            );
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains(messageContains.toLowerCase()));
        }
    }

    private static final class HeapBytesSource implements BytesSource {
        private final byte[] data;

        private HeapBytesSource(byte[] data) {
            this.data = data;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(data, index, dst, dstOff, len);
        }
    }
}
