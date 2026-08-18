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
}
