package yier.bubu.redis.storage.api.result;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

    @Test
    public void poppedSequenceReportsCountEncodedBytesRetainedBytesAndClosesOnce() {
        AtomicInteger closes = new AtomicInteger();
        PoppedValueSequence values = TestPoppedValueSequence.of(
                List.of("a", "bc"),
                23L,
                closes::incrementAndGet
        );
        CapturingListSink sink = new CapturingListSink();

        Assert.assertFalse(values.isNull());
        Assert.assertEquals(2, values.count());
        Assert.assertEquals(15L, values.encodedElementBytes());
        Assert.assertEquals(23L, values.retainedMemoryBytes());
        values.emitTo(sink);
        Assert.assertEquals(List.of("a", "bc"), sink.values);

        values.close();
        values.close();
        Assert.assertEquals(1, closes.get());
    }

    @Test
    public void poppedSequenceSeparatesNullFromEmpty() {
        PoppedValueSequence nullValue = TestPoppedValueSequence.nullValue();
        PoppedValueSequence empty = TestPoppedValueSequence.empty();

        Assert.assertTrue(nullValue.isNull());
        Assert.assertEquals(0, nullValue.count());
        Assert.assertEquals(0L, nullValue.encodedElementBytes());
        Assert.assertEquals(0L, nullValue.retainedMemoryBytes());
        Assert.assertFalse(empty.isNull());
        Assert.assertEquals(0, empty.count());
        Assert.assertEquals(0L, empty.encodedElementBytes());
        Assert.assertEquals(0L, empty.retainedMemoryBytes());
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

    private static final class CapturingListSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void bulkString(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            values.add(new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            bulkString(data);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value));
        }
    }

    private static final class TestPoppedValueSequence implements PoppedValueSequence {
        private final List<byte[]> values;
        private final boolean nullValue;
        private final long retainedMemoryBytes;
        private final Runnable closeOwner;
        private boolean closed;

        private TestPoppedValueSequence(
                List<byte[]> values,
                boolean nullValue,
                long retainedMemoryBytes,
                Runnable closeOwner
        ) {
            this.values = values;
            this.nullValue = nullValue;
            this.retainedMemoryBytes = retainedMemoryBytes;
            this.closeOwner = closeOwner;
        }

        static PoppedValueSequence nullValue() {
            return new TestPoppedValueSequence(List.of(), true, 0L, () -> {
            });
        }

        static PoppedValueSequence empty() {
            return new TestPoppedValueSequence(List.of(), false, 0L, () -> {
            });
        }

        static PoppedValueSequence of(List<String> values, long retainedMemoryBytes, Runnable closeOwner) {
            List<byte[]> bytes = new ArrayList<>(values.size());
            for (String value : values) {
                bytes.add(value.getBytes(StandardCharsets.UTF_8));
            }
            return new TestPoppedValueSequence(bytes, false, retainedMemoryBytes, closeOwner);
        }

        @Override
        public boolean isNull() {
            return nullValue;
        }

        @Override
        public long encodedElementBytes() {
            long total = 0L;
            for (byte[] value : values) {
                total += 1L + decimalDigits(value.length) + 2L + value.length + 2L;
            }
            return total;
        }

        @Override
        public long retainedMemoryBytes() {
            return retainedMemoryBytes;
        }

        @Override
        public int count() {
            return values.size();
        }

        @Override
        public void emitTo(BulkStringSink out) {
            for (byte[] value : values) {
                out.bulkString(value);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            closeOwner.run();
        }

        private static int decimalDigits(int value) {
            int digits = 1;
            int v = value;
            while (v >= 10) {
                v /= 10;
                digits++;
            }
            return digits;
        }
    }
}
