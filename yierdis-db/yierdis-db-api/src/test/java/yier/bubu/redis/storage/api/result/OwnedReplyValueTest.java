package yier.bubu.redis.storage.api.result;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.StringWriteOps;

public class OwnedReplyValueTest {
    @Test
    public void ownedBulkStringReportsPayloadRetainedBytesAndClosesOwnerOnce() {
        AtomicInteger closes = new AtomicInteger();
        BulkStringValue value = BulkStringValue.owned(slice("old"), 3, 17, closes::incrementAndGet);
        CapturingSink sink = new CapturingSink();

        Assert.assertFalse(value.isNull());
        Assert.assertEquals(3, value.payloadLength());
        Assert.assertEquals(17L, value.retainedMemoryBytes());
        value.writeTo(sink);
        Assert.assertEquals("old", sink.value);

        value.close();
        value.close();
        Assert.assertEquals(1, closes.get());
    }

    @Test
    public void setStringValueDelegatesCloseToOldValue() {
        AtomicInteger closes = new AtomicInteger();
        BulkStringValue old = BulkStringValue.owned(slice("one"), 3, 11, closes::incrementAndGet);
        StringWriteOps.SetStringValue result = new StringWriteOps.SetStringValue(true, old);

        Assert.assertTrue(result.applied());
        Assert.assertSame(old, result.oldValue());
        result.close();
        result.close();
        Assert.assertEquals(1, closes.get());
    }

    @Test
    public void nullBulkStringHasMinusOnePayloadAndNoRetainedBytes() {
        BulkStringValue value = BulkStringValue.nullValue();
        Assert.assertTrue(value.isNull());
        Assert.assertEquals(-1, value.payloadLength());
        Assert.assertEquals(0L, value.retainedMemoryBytes());
        value.close();
    }

    private static BytesSlice slice(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new BytesSlice() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }

            @Override
            public void getBytes(int srcPos, byte[] dst, int dstPos, int len) {
                System.arraycopy(bytes, srcPos, dst, dstPos, len);
            }

            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(bytes);
            }
        };
    }

    private static final class CapturingSink implements BulkStringSink {
        private String value;

        @Override
        public void bulkString(byte[] data) {
            value = data == null ? null : new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            value = new String(data, off, len, StandardCharsets.UTF_8);
        }

        @Override
        public void bulkString(BytesSlice slice) {
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            bulkString(data);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            this.value = Long.toString(value);
        }
    }
}
