package yier.bubu.redis.storage.api.result;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;

public class MeasuredReplySourceTest {
    @Test
    public void metricsIncludeNullBinaryLargeAndIntegerBoundaryValues() {
        BulkStringMetrics metrics = new BulkStringMetrics();
        byte[] binary = new byte[]{0, (byte) 0xFF, '\n'};
        byte[] large = new byte[1_024];

        metrics.bulkString((byte[]) null);
        metrics.bulkString(binary);
        metrics.bulkString(large);
        metrics.bulkStringLongAscii(Long.MIN_VALUE);

        Assert.assertEquals(4, metrics.count());
        Assert.assertEquals(
                encoded(null) + encoded(binary) + encoded(large) + encoded(Long.toString(Long.MIN_VALUE).getBytes(StandardCharsets.US_ASCII)),
                metrics.encodedElementBytes()
        );
    }

    @Test
    public void measuredSequenceAndMapReplayTheirDeclaredElementsUntilClosed() {
        byte[] field = "field".getBytes(StandardCharsets.US_ASCII);
        byte[] value = "value".getBytes(StandardCharsets.US_ASCII);
        MeasuredBulkStringSequence sequence = MeasuredBulkStringSequences.of(
                2,
                encoded(field) + encoded(value),
                0L,
                out -> {
                    out.bulkString(field);
                    out.bulkString(value);
                }
        );
        BulkStringMapMetrics map = BulkStringMapMetricsSources.of(
                1,
                encoded(field) + encoded(value),
                0L,
                out -> {
                    out.bulkString(field);
                    out.bulkString(value);
                }
        );

        RecordingSink sequenceOutput = new RecordingSink();
        sequence.emitTo(sequenceOutput);
        Assert.assertEquals(List.of("field", "value"), sequenceOutput.values());

        RecordingSink mapOutput = new RecordingSink();
        map.emitPairsTo(mapOutput);
        Assert.assertEquals(1, map.pairCount());
        Assert.assertEquals(List.of("field", "value"), mapOutput.values());

        sequence.close();
        map.close();
        Assert.assertThrows(IllegalStateException.class, () -> sequence.emitTo(new RecordingSink()));
        Assert.assertThrows(IllegalStateException.class, () -> map.emitPairsTo(new RecordingSink()));
    }

    private static long encoded(byte[] value) {
        if (value == null) {
            return 5L;
        }
        return 1L + decimalDigits(value.length) + 2L + value.length + 2L;
    }

    private static int decimalDigits(int value) {
        int digits = 1;
        while (value >= 10) {
            value /= 10;
            digits++;
        }
        return digits;
    }

    private static final class RecordingSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void bulkString(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.ISO_8859_1));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.ISO_8859_1));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            bulkString(bytes);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value));
        }

        private List<String> values() {
            return values;
        }
    }
}
