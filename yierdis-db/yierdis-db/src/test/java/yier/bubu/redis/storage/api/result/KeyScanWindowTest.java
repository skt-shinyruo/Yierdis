package yier.bubu.redis.storage.api.result;

import java.util.function.IntConsumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.ScanCursorV2;

public class KeyScanWindowTest {
    @Test
    public void replayableWindowExposesTheRequiredLifecycleAndMetadata() {
        RecordingWindow window = new RecordingWindow();

        Assert.assertEquals(3, window.elementCount());
        Assert.assertEquals(List.of(3, -1, 5), lengths(window));
        Assert.assertEquals(8L, window.inspectedSlots());
        Assert.assertEquals(123L, window.tableGeneration());
        Assert.assertEquals(456L, window.expiryEvaluationMillis());
        Assert.assertEquals(ScanCursorV2.of(4, 1, 2L).value(), window.nextCursor().value());
        Assert.assertTrue(window.current());

        window.close();
        window.close();
        Assert.assertTrue(window.closed.get());
    }

    private static final class RecordingWindow implements KeyScanWindow {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public ScanCursorV2 nextCursor() {
            return ScanCursorV2.of(4, 1, 2L);
        }

        @Override
        public long inspectedSlots() {
            return 8L;
        }

        @Override
        public long tableGeneration() {
            return 123L;
        }

        @Override
        public long expiryEvaluationMillis() {
            return 456L;
        }

        @Override
        public boolean current() {
            return true;
        }

        @Override
        public int elementCount() {
            return 3;
        }

        @Override
        public long retainedMemoryBytes() {
            return 0L;
        }

        @Override
        public void visitElementLengths(IntConsumer out) {
            out.accept(3);
            out.accept(-1);
            out.accept(5);
        }

        @Override
        public void emitTo(ByteValueSink out) {
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static List<Integer> lengths(ByteSequenceSource source) {
        List<Integer> lengths = new ArrayList<>();
        source.visitElementLengths(lengths::add);
        return lengths;
    }
}
