package yier.bubu.redis.storage.api.result;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.ScanCursorV2;

public class KeyScanWindowTest {
    @Test
    public void replayableWindowExposesTheRequiredLifecycleAndMetadata() {
        RecordingWindow window = new RecordingWindow();

        Assert.assertEquals(3, window.count());
        Assert.assertEquals(27L, window.encodedElementBytes());
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
        public long encodedElementBytes() {
            return 27L;
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
        public int count() {
            return 3;
        }

        @Override
        public void emitTo(BulkStringSink out) {
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
