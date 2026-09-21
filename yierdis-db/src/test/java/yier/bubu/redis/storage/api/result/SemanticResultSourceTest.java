package yier.bubu.redis.storage.api.result;

import java.util.function.IntConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class SemanticResultSourceTest {
    @Test
    public void sequenceLengthsAreRepeatableOrderedAndNonConsuming() {
        AtomicInteger emissions = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        ByteSequenceSource source = new ByteSequenceSource() {
            @Override
            public int elementCount() {
                return 3;
            }

            @Override
            public long retainedMemoryBytes() {
                return 17L;
            }

            @Override
            public void visitElementLengths(IntConsumer out) {
                out.accept(3);
                out.accept(-1);
                out.accept(5);
            }

            @Override
            public void emitTo(ByteValueSink out) {
                emissions.incrementAndGet();
                out.value(new byte[]{1, 2, 3});
                out.nullValue();
                out.value(new byte[]{4, 5, 6, 7, 8});
            }

            @Override
            public void close() {
                closes.compareAndSet(0, 1);
            }
        };

        Assert.assertEquals(List.of(3, -1, 5), lengths(source::visitElementLengths));
        Assert.assertEquals(List.of(3, -1, 5), lengths(source::visitElementLengths));
        Assert.assertEquals(0, emissions.get());
        source.emitTo(new CountingSink());
        Assert.assertEquals(1, emissions.get());
        source.close();
        source.close();
        Assert.assertEquals(1, closes.get());
    }

    @Test
    public void mapLengthsAreFieldValueOrdered() {
        ByteMapSource source = new ByteMapSource() {
            @Override
            public int pairCount() {
                return 2;
            }

            @Override
            public long retainedMemoryBytes() {
                return 23L;
            }

            @Override
            public void visitPairLengths(IntConsumer out) {
                out.accept(2);
                out.accept(4);
                out.accept(3);
                out.accept(-1);
            }

            @Override
            public void emitPairsTo(ByteValueSink out) {
            }

            @Override
            public void close() {
            }
        };

        Assert.assertEquals(List.of(2, 4, 3, -1), lengths(source::visitPairLengths));
        Assert.assertEquals(2, source.pairCount());
        Assert.assertEquals(23L, source.retainedMemoryBytes());
    }

    @Test
    public void copiedFromFreezesEmitterResults() {
        List<byte[]> live = new ArrayList<>();
        live.add(new byte[]{'a'});
        live.add(new byte[]{'b'});
        live.add(new byte[]{'c'});
        ByteSequenceSource source = ByteSequenceSources.copiedFrom(out -> {
            for (byte[] item : live) {
                out.value(item);
            }
        });
        live.remove(0);
        Assert.assertEquals(3, source.elementCount());
        Assert.assertEquals(3L, source.retainedMemoryBytes());
        Assert.assertEquals(List.of(1, 1, 1), lengths(source::visitElementLengths));
        RecordingValues sink = new RecordingValues();
        source.emitTo(sink);
        Assert.assertEquals(List.of("a", "b", "c"), sink.values);
    }

    @Test
    public void copiedMapFromRejectsOddEmitterLength() {
        try {
            ByteMapSources.copiedFrom(out -> out.value(new byte[]{'a'}));
            Assert.fail("expected odd captured map to fail");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("odd"));
        }
    }

    @Test
    public void copiedFromPreservesLongAsciiAndNulls() {
        ByteSequenceSource source = ByteSequenceSources.copiedFrom(out -> {
            out.longAscii(Long.MIN_VALUE);
            out.nullValue();
            out.value(new byte[]{1, 2});
        });
        Assert.assertEquals(3, source.elementCount());
        Assert.assertEquals(List.of(20, -1, 2), lengths(source::visitElementLengths));
        Assert.assertEquals(22L, source.retainedMemoryBytes());
    }

    private static List<Integer> lengths(LengthVisit visit) {
        List<Integer> values = new ArrayList<>();
        visit.accept(values::add);
        return values;
    }

    @FunctionalInterface
    private interface LengthVisit {
        void accept(IntConsumer out);
    }

    private static final class CountingSink implements ByteValueSink {
        @Override
        public void value(byte[] data) {
        }

        @Override
        public void value(byte[] data, int offset, int length) {
        }

        @Override
        public void value(yier.bubu.redis.bytes.BytesSlice slice) {
        }

        @Override
        public void longAscii(long value) {
        }

        @Override
        public void nullValue() {
        }
    }

    private static final class RecordingValues implements ByteValueSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data));
        }

        @Override
        public void value(byte[] data, int offset, int length) {
            values.add(data == null ? null : new String(data, offset, length));
        }

        @Override
        public void value(yier.bubu.redis.bytes.BytesSlice slice) {
            throw new UnsupportedOperationException("snapshot emit should not use live slices");
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            values.add(null);
        }
    }
}
