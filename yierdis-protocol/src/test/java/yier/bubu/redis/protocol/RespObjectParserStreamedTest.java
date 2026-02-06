package yier.bubu.redis.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespObjectParserStreamedTest {
    @Test
    public void parsesStreamedBlobStringAsBulkString() {
        byte[] bytes = ascii("$?\r\n;5\r\nhello\r\n;0\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            RespObject obj = RespObjectParser.parse(frame);
            Assert.assertTrue(obj instanceof RespBulkString);
            RespBulkString bs = (RespBulkString) obj;
            Assert.assertArrayEquals(ascii("hello"), bs.data());
        }
    }

    @Test
    public void parsesStreamedArrayUntilEndMarker() {
        byte[] bytes = ascii("*?\r\n+PING\r\n:1\r\n.\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            RespObject obj = RespObjectParser.parse(frame);
            Assert.assertTrue(obj instanceof RespArray);
            RespArray arr = (RespArray) obj;
            List<RespObject> values = arr.values();
            Assert.assertNotNull(values);
            Assert.assertEquals(2, values.size());
            Assert.assertTrue(values.get(0) instanceof RespSimpleString);
            Assert.assertEquals("PING", ((RespSimpleString) values.get(0)).value());
            Assert.assertTrue(values.get(1) instanceof RespInteger);
            Assert.assertEquals(1L, ((RespInteger) values.get(1)).value());
        }
    }

    @Test
    public void parsesNestedStreamedBlobStringInsideStreamedArray() {
        byte[] bytes = ascii("*?\r\n$?\r\n;3\r\nfoo\r\n;0\r\n.\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            RespObject obj = RespObjectParser.parse(frame);
            Assert.assertTrue(obj instanceof RespArray);
            RespArray arr = (RespArray) obj;
            Assert.assertEquals(1, arr.values().size());
            Assert.assertTrue(arr.values().get(0) instanceof RespBulkString);
            Assert.assertArrayEquals(ascii("foo"), ((RespBulkString) arr.values().get(0)).data());
        }
    }

    @Test
    public void parsesStreamedMapAndRejectsOddElements() {
        byte[] ok = ascii("%?\r\n+key\r\n+value\r\n.\r\n");
        try (RespFrame frame = new HeapRespFrame(ok)) {
            RespObject obj = RespObjectParser.parse(frame);
            Assert.assertTrue(obj instanceof RespMap);
            RespMap map = (RespMap) obj;
            Assert.assertEquals(1, map.entries().size());
            RespMap.Entry e = map.entries().get(0);
            Assert.assertTrue(e.key() instanceof RespSimpleString);
            Assert.assertTrue(e.value() instanceof RespSimpleString);
            Assert.assertEquals("key", ((RespSimpleString) e.key()).value());
            Assert.assertEquals("value", ((RespSimpleString) e.value()).value());
        }

        byte[] bad = ascii("%?\r\n+key\r\n.\r\n");
        try (RespFrame frame = new HeapRespFrame(bad)) {
            try {
                RespObjectParser.parse(frame);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().toLowerCase().contains("map value"));
            }
        }
    }

    @Test
    public void streamedBlobStringRespectsMaxBulkBytes() {
        byte[] bytes = ascii("$?\r\n;4\r\nabcd\r\n;0\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            try {
                RespObjectParser.parse(frame, 3, 1024, 64, 1024);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().toLowerCase().contains("bulk length too large"));
            }
        }
    }

    @Test
    public void streamedArrayRespectsMaxArrayLen() {
        byte[] bytes = ascii("*?\r\n+PING\r\n+PONG\r\n.\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            try {
                RespObjectParser.parse(frame, 1024, 1, 64, 1024);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().toLowerCase().contains("array length too large"));
            }
        }
    }

    @Test
    public void parsesAttributesWrappingStreamedValue() {
        byte[] bytes = ascii("|1\r\n+meta\r\n:1\r\n$?\r\n;5\r\nhello\r\n;0\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            RespObject obj = RespObjectParser.parse(frame);
            Assert.assertTrue(obj instanceof RespAttribute);

            RespAttribute attr = (RespAttribute) obj;
            Assert.assertNotNull(attr.attributes());
            Assert.assertEquals(1, attr.attributes().entries().size());

            RespMap.Entry e = attr.attributes().entries().get(0);
            Assert.assertTrue(e.key() instanceof RespSimpleString);
            Assert.assertEquals("meta", ((RespSimpleString) e.key()).value());
            Assert.assertTrue(e.value() instanceof RespInteger);
            Assert.assertEquals(1L, ((RespInteger) e.value()).value());

            Assert.assertTrue(attr.value() instanceof RespBulkString);
            Assert.assertArrayEquals(ascii("hello"), ((RespBulkString) attr.value()).data());
        }
    }

    @Test
    public void parsesChainedAttributesWrapperAsNestedRespAttributeObjects() {
        byte[] bytes = ascii("|0\r\n|0\r\n+OK\r\n");
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            RespObject obj = RespObjectParser.parse(frame);
            Assert.assertTrue(obj instanceof RespAttribute);

            RespAttribute outer = (RespAttribute) obj;
            Assert.assertEquals(0, outer.attributes().entries().size());
            Assert.assertTrue(outer.value() instanceof RespAttribute);

            RespAttribute inner = (RespAttribute) outer.value();
            Assert.assertEquals(0, inner.attributes().entries().size());
            Assert.assertTrue(inner.value() instanceof RespSimpleString);
            Assert.assertEquals("OK", ((RespSimpleString) inner.value()).value());
        }
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class HeapRespFrame implements RespFrame {
        private final byte[] data;

        private HeapRespFrame(byte[] data) {
            this.data = data;
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void close() {
            // heap frame: no-op
        }
    }
}
