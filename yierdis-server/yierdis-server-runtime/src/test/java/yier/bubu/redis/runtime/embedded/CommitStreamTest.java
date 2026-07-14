package yier.bubu.redis.runtime.embedded;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.CommandRecordView;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbCommitReservation;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;

public class CommitStreamTest {
    @Test
    public void publishesCommittedRecordsInReservationOrderAcrossDatabases() throws Exception {
        List<Long> sequences = Collections.synchronizedList(new ArrayList<>());
        List<Integer> databases = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch delivered = new CountDownLatch(3);
        try (CommitStream stream = new CommitStream(event -> {
            sequences.add(event.sequence());
            databases.add(event.dbIndex());
            Assert.assertEquals(YierdisChangeKind.USER_COMMAND, event.kind());
            delivered.countDown();
        }, 4, 4096L, 1000L)) {
            publish(stream, 1, "SET", "a", "1");
            publish(stream, 0, "HSET", "h", "f", "v");
            publish(stream, 1, "DEL", "a");

            Assert.assertTrue(delivered.await(2L, TimeUnit.SECONDS));
            Assert.assertEquals(List.of(1L, 2L, 3L), sequences);
            Assert.assertEquals(List.of(1, 0, 1), databases);
            Assert.assertEquals(3L, stream.stats().lastAssignedSequence());
            Assert.assertEquals(3L, stream.stats().lastAcknowledgedSequence());
        }
    }

    @Test
    public void canceledAndStaleReservationsDoNotConsumeOrReleaseNewSlots() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        try (CommitStream stream = new CommitStream(event -> delivered.countDown(), 1, 4096L, 1000L)) {
            ByteArrayCommandRecord firstRecord = record("SET", "a", "1");
            DbCommitReservation first = stream.reserve(0, DbCommitKind.USER, firstRecord, 1L, 1L);
            firstRecord.close();
            first.close();
            first.close();

            ByteArrayCommandRecord secondRecord = record("SET", "b", "2");
            DbCommitReservation second = stream.reserve(0, DbCommitKind.USER, secondRecord, 1L, 2L);
            secondRecord.close();
            first.close();
            Assert.assertEquals(1L, stream.stats().reservedEvents());
            Assert.assertEquals(1L, stream.publish(second));
            Assert.assertTrue(delivered.await(2L, TimeUnit.SECONDS));
            Assert.assertEquals(0L, stream.stats().reservedEvents());
        }
    }

    @Test
    public void capacityAndFailedStreamRejectBeforeAssigningAnotherSequence() throws Exception {
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        try (CommitStream stream = new CommitStream(event -> {
            callbackEntered.countDown();
            try {
                Assert.assertTrue(releaseCallback.await(2L, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }, 2, 512L, 1000L)) {
            ByteArrayCommandRecord firstRecord = record("SET", "a", "1");
            DbCommitReservation first = stream.reserve(0, DbCommitKind.USER, firstRecord, 1L, 1L);
            firstRecord.close();
            stream.publish(first);
            Assert.assertTrue(callbackEntered.await(2L, TimeUnit.SECONDS));

            ByteArrayCommandRecord secondRecord = record("SET", "b", "2");
            DbCommitReservation second = stream.reserve(0, DbCommitKind.USER, secondRecord, 1L, 2L);
            secondRecord.close();
            stream.publish(second);

            ByteArrayCommandRecord thirdRecord = record("SET", "c", "3");
            try {
                Assert.assertThrows(DbCommitStreamUnavailableException.class,
                        () -> stream.reserve(0, DbCommitKind.USER, thirdRecord, 1L, 3L));
            } finally {
                thirdRecord.close();
            }
            Assert.assertEquals(2L, stream.stats().lastAssignedSequence());
            Assert.assertEquals(1L, stream.stats().rejectedWrites());
            releaseCallback.countDown();
        }
    }

    @Test
    public void failAfterCommitRetainsTheReservedRecordAndFailsTheStream() {
        try (CommitStream stream = new CommitStream(event -> Assert.fail("held failed reservation must not be delivered"), 2, 4096L, 1000L)) {
            ByteArrayCommandRecord record = record("SET", "a", "1");
            DbCommitReservation reservation = stream.reserve(0, DbCommitKind.USER, record, 1L, 1L);
            record.close();

            stream.failAfterCommit(reservation);
            stream.failAfterCommit(reservation);

            CommitStreamStats stats = stream.stats();
            Assert.assertEquals(CommitStreamState.FAILED, stats.state());
            Assert.assertEquals("DbPostCommitInvariantFailure", stats.firstFailureType());
            Assert.assertEquals(1L, stats.reservedEvents());
            Assert.assertEquals(0L, stats.lastAssignedSequence());
            Assert.assertThrows(DbCommitStreamUnavailableException.class,
                    () -> stream.reserve(0, DbCommitKind.USER, record("SET", "b", "2"), 1L, 2L));
        }
    }

    @Test
    public void noopPublisherConsumesNeitherCapacityNorSequence() {
        Assert.assertFalse(DbCommitPublisher.NOOP.enabled());
        Assert.assertEquals(0L, DbCommitPublisher.NOOP.publish(DbCommitReservation.NOOP));
    }

    @Test
    public void callbackReceivesABorrowedViewThatExpiresAfterDelivery() throws Exception {
        AtomicReference<CommandRecordView> borrowed = new AtomicReference<>();
        AtomicReference<Throwable> foreignFailure = new AtomicReference<>();
        CountDownLatch delivered = new CountDownLatch(1);
        try (CommitStream stream = new CommitStream(event -> {
            CommandRecordView view = event.request();
            borrowed.set(view);
            Assert.assertArrayEquals(bytes("SET"), view.toByteArray(0));
            Thread foreign = Thread.ofPlatform().start(() -> {
                try {
                    view.argc();
                } catch (Throwable t) {
                    foreignFailure.set(t);
                }
            });
            try {
                foreign.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            delivered.countDown();
        }, 2, 4096L, 1000L)) {
            publish(stream, 0, "SET", "a", "1");
            Assert.assertTrue(delivered.await(2L, TimeUnit.SECONDS));
            waitForAcknowledgement(stream, 1L);
            Assert.assertTrue(foreignFailure.get() instanceof IllegalStateException);
            Assert.assertThrows(IllegalStateException.class, () -> borrowed.get().argc());
        }
    }

    @Test
    public void sequenceExhaustionFailsBeforeAnyPublication() {
        try (CommitStream stream = new CommitStream(event -> { }, 1, 4096L, 1000L, Long.MAX_VALUE)) {
            ByteArrayCommandRecord record = record("SET", "a", "1");
            try {
                Assert.assertThrows(DbCommitStreamUnavailableException.class,
                        () -> stream.reserve(0, DbCommitKind.USER, record, 1L, 1L));
            } finally {
                record.close();
            }
            Assert.assertEquals(Long.MAX_VALUE, stream.stats().lastAssignedSequence());
        }
    }

    private static void publish(CommitStream stream, int dbIndex, String... argv) {
        ByteArrayCommandRecord record = record(argv);
        DbCommitReservation reservation = stream.reserve(dbIndex, DbCommitKind.USER, record, 1L, 1L);
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void waitForAcknowledgement(CommitStream stream, long sequence) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (stream.stats().lastAcknowledgedSequence() < sequence) {
            if (System.nanoTime() >= deadline) {
                Assert.fail("timed out waiting for commit acknowledgement");
            }
            Thread.sleep(1L);
        }
    }

}
