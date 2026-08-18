package yier.bubu.redis.storage.api.result;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.StringOps;

import static yier.bubu.redis.storage.testkit.TestBytes.slice;

public class ByteValueTest {
    @Test
    public void nullArraySliceAndLongExposeSemanticPayloads() {
        RecordingSink sink = new RecordingSink();

        ByteValue nullValue = ByteValue.NULL;
        Assert.assertEquals(-1, nullValue.payloadLength());
        nullValue.emitTo(sink);
        Assert.assertEquals("null", sink.kind);

        ByteValue array = ByteValue.bytes(bytes("abcd"), 1, 2);
        Assert.assertTrue(array.payloadLength() >= 0);
        Assert.assertEquals(2, array.payloadLength());
        array.emitTo(sink);
        Assert.assertEquals("array", sink.kind);
        Assert.assertEquals("bc", sink.value);

        ByteValue slice = ByteValue.slice(slice(bytes("slice")));
        Assert.assertEquals(5, slice.payloadLength());
        slice.emitTo(sink);
        Assert.assertEquals("slice", sink.kind);
        Assert.assertEquals("slice", sink.value);

        ByteValue number = ByteValue.longAscii(Long.MIN_VALUE);
        Assert.assertEquals(20, number.payloadLength());
        number.emitTo(sink);
        Assert.assertEquals("long", sink.kind);
        Assert.assertEquals(Long.toString(Long.MIN_VALUE), sink.value);
    }

    @Test
    public void arrayWindowChecksBounds() {
        byte[] data = bytes("abc");
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> ByteValue.bytes(data, -1, 1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> ByteValue.bytes(data, 2, 2));
    }

    @Test
    public void ownedValueReportsRetainedBytesAndClosesOwnerOnce() {
        AtomicInteger closes = new AtomicInteger();
        ByteValue value = ByteValue.owned(
                slice(bytes("owned")),
                5,
                37L,
                closes::incrementAndGet
        );

        Assert.assertEquals(5, value.payloadLength());
        Assert.assertEquals(37L, value.retainedMemoryBytes());
        value.close();
        value.close();
        Assert.assertEquals(1, closes.get());
    }

    @Test
    public void setStringValueClosesItsOldValue() {
        AtomicInteger closes = new AtomicInteger();
        ByteValue oldValue = ByteValue.owned(
                slice(bytes("old")),
                3,
                11L,
                closes::incrementAndGet
        );
        StringOps.SetStringValue result = new StringOps.SetStringValue(true, oldValue);

        result.close();
        result.close();

        Assert.assertEquals(1, closes.get());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingSink implements ByteValueSink {
        private String kind;
        private String value;

        @Override
        public void value(byte[] data) {
            kind = "array";
            value = new String(data, StandardCharsets.US_ASCII);
        }

        @Override
        public void value(byte[] data, int offset, int length) {
            kind = "array";
            value = new String(data, offset, length, StandardCharsets.US_ASCII);
        }

        @Override
        public void value(BytesSlice slice) {
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            kind = "slice";
            value = new String(data, StandardCharsets.US_ASCII);
        }

        @Override
        public void longAscii(long number) {
            kind = "long";
            value = Long.toString(number);
        }

        @Override
        public void nullValue() {
            kind = "null";
            value = null;
        }
    }
}
