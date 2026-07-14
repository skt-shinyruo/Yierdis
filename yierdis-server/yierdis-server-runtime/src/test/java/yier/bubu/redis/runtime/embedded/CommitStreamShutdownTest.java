package yier.bubu.redis.runtime.embedded;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitReservation;

public class CommitStreamShutdownTest {
    @Test
    public void shutdownTimeoutLeavesInFlightRecordOwnedUntilCallbackReturns() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<TrackingRecord> source = new AtomicReference<>();
        CommitStream stream = new CommitStream(event -> {
            entered.countDown();
            boolean interrupted = false;
            while (release.getCount() != 0L) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }, 1, 4096L, 25L);
        try {
            TrackingRecord record = new TrackingRecord("SET", "a", "1");
            source.set(record);
            DbCommitReservation reservation = stream.reserve(0, DbCommitKind.USER, record, 1L, 1L);
            record.close();
            stream.publish(reservation);
            Assert.assertTrue(entered.await(2L, TimeUnit.SECONDS));

            Assert.assertFalse(stream.shutdownGracefully(Duration.ofMillis(25L)));
            Assert.assertTrue(stream.stats().shutdownTimedOut());
            Assert.assertEquals(1, source.get().closeCount.get());

            release.countDown();
            waitForTerminalCleanup(stream);
            Assert.assertEquals(CommitStreamState.FAILED, stream.state());
            Assert.assertEquals(2, source.get().closeCount.get());
        } finally {
            release.countDown();
            stream.close();
        }
    }

    @Test
    public void shutdownGracefullyAcknowledgesEveryPublishedRecordBeforeClosing() throws Exception {
        AtomicInteger delivered = new AtomicInteger();
        CommitStream stream = new CommitStream(event -> delivered.incrementAndGet(), 2, 4096L, 1000L);
        try {
            publish(stream, "SET", "a", "1");

            Assert.assertTrue(stream.shutdownGracefully(Duration.ofSeconds(1L)));
            Assert.assertEquals(1, delivered.get());
            Assert.assertEquals(CommitStreamState.CLOSED, stream.state());
            Assert.assertEquals(0L, stream.stats().reservedEvents());
            Assert.assertEquals(1L, stream.stats().lastAcknowledgedSequence());
            Assert.assertTrue(stream.shutdownGracefully(Duration.ofMillis(1L)));
        } finally {
            stream.close();
        }
    }

    @Test
    public void shutdownGracefullyFailsOutstandingReservationInsteadOfSilentlyCancellingIt() {
        CommitStream stream = new CommitStream(event -> Assert.fail("unpublished reservation must not reach the sink"), 1, 4096L, 1000L);
        try {
            TrackingRecord record = new TrackingRecord("SET", "a", "1");
            DbCommitReservation reservation = stream.reserve(0, DbCommitKind.USER, record, 1L, 1L);
            record.close();

            Assert.assertFalse(stream.shutdownGracefully(Duration.ofSeconds(1L)));
            CommitStreamStats stats = stream.stats();
            Assert.assertEquals(CommitStreamState.FAILED, stats.state());
            Assert.assertEquals("CommitStreamOutstandingReservation", stats.firstFailureType());
            Assert.assertEquals(0L, stats.reservedEvents());
            Assert.assertEquals(2, record.closeCount.get());

            reservation.close();
            Assert.assertEquals(2, record.closeCount.get());
        } finally {
            stream.close();
        }
    }

    @Test
    public void shutdownGracefullyCleansReturnedFailedCallbackWithoutAcknowledgingIt() throws Exception {
        AtomicReference<TrackingRecord> source = new AtomicReference<>();
        CommitStream stream = new CommitStream(event -> {
            throw new IllegalStateException("sink failed\nwith a diagnostic");
        }, 1, 4096L, 1000L);
        try {
            TrackingRecord record = new TrackingRecord("SET", "a", "1");
            source.set(record);
            DbCommitReservation reservation = stream.reserve(0, DbCommitKind.USER, record, 1L, 1L);
            record.close();
            stream.publish(reservation);

            waitForState(stream, CommitStreamState.FAILED);
            Assert.assertFalse(stream.shutdownGracefully(Duration.ofSeconds(1L)));
            CommitStreamStats stats = stream.stats();
            Assert.assertEquals("IllegalStateException", stats.firstFailureType());
            Assert.assertEquals("sink failed with a diagnostic", stats.firstFailureMessage());
            Assert.assertEquals(0L, stats.reservedEvents());
            Assert.assertEquals(0L, stats.lastAcknowledgedSequence());
            Assert.assertEquals(2, source.get().closeCount.get());
        } finally {
            stream.close();
        }
    }

    @Test
    public void shutdownGracefullyInterruptsBlockedCallbackAndLetsWorkerOwnFinalCleanup() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<TrackingRecord> source = new AtomicReference<>();
        CommitStream stream = new CommitStream(event -> {
            entered.countDown();
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, 1, 4096L, 25L);
        try {
            TrackingRecord record = new TrackingRecord("SET", "a", "1");
            source.set(record);
            DbCommitReservation reservation = stream.reserve(0, DbCommitKind.USER, record, 1L, 1L);
            record.close();
            stream.publish(reservation);
            Assert.assertTrue(entered.await(2L, TimeUnit.SECONDS));

            Assert.assertFalse(stream.shutdownGracefully(Duration.ofMillis(25L)));
            waitForTerminalCleanup(stream);
            Assert.assertEquals(CommitStreamState.FAILED, stream.state());
            Assert.assertEquals("CommitStreamDrainTimeout", stream.stats().firstFailureType());
            Assert.assertEquals(2, source.get().closeCount.get());
        } finally {
            stream.close();
        }
    }

    private static void publish(CommitStream stream, String... argv) {
        ByteArrayCommandRecord record = record(argv);
        DbCommitReservation reservation = stream.reserve(0, DbCommitKind.USER, record, 1L, 1L);
        record.close();
        stream.publish(reservation);
    }

    private static ByteArrayCommandRecord record(String... argv) {
        byte[][] bytes = new byte[argv.length][];
        for (int i = 0; i < argv.length; i++) {
            bytes[i] = argv[i].getBytes(StandardCharsets.US_ASCII);
        }
        return ByteArrayCommandRecord.copyOf(bytes);
    }

    private static void waitForTerminalCleanup(CommitStream stream) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (stream.stats().reservedEvents() != 0L) {
            if (System.nanoTime() >= deadline) {
                Assert.fail("timed out waiting for commit stream terminal cleanup");
            }
            Thread.sleep(1L);
        }
    }

    private static void waitForState(CommitStream stream, CommitStreamState expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (stream.state() != expected) {
            if (System.nanoTime() >= deadline) {
                Assert.fail("timed out waiting for commit stream state " + expected);
            }
            Thread.sleep(1L);
        }
    }

    private static final class TrackingRecord implements ImmutableCommandRecord {
        private final ImmutableCommandRecord delegate;
        private final AtomicInteger closeCount;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TrackingRecord(String... argv) {
            this(record(argv), new AtomicInteger());
        }

        private TrackingRecord(ImmutableCommandRecord delegate, AtomicInteger closeCount) {
            this.delegate = delegate;
            this.closeCount = closeCount;
        }

        @Override
        public int argc() {
            return delegate.argc();
        }

        @Override
        public boolean isNull(int index) {
            return delegate.isNull(index);
        }

        @Override
        public int len(int index) {
            return delegate.len(index);
        }

        @Override
        public byte byteAt(int index, int offset) {
            return delegate.byteAt(index, offset);
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            delegate.copyToByteArray(index, dst, dstOff);
        }

        @Override
        public long retainedMemoryBytes() {
            return delegate.retainedMemoryBytes();
        }

        @Override
        public TrackingRecord retain() {
            return new TrackingRecord(delegate.retain(), closeCount);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                closeCount.incrementAndGet();
                delegate.close();
            }
        }
    }
}
