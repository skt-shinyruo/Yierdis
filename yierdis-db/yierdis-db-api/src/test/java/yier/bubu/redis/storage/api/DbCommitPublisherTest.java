package yier.bubu.redis.storage.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.CommandRecordView;

public class DbCommitPublisherTest {
    @Test
    public void eventExposesOnlyBorrowedRecordDataAndMapsSyntheticKinds() {
        CommandRecordView record = ByteArrayCommandRecord.copyOf(new byte[]{'D', 'E', 'L'});
        DbCommitEvent user = event(DbCommitKind.USER, record);
        DbCommitEvent expired = event(DbCommitKind.EXPIRED, record);

        Assert.assertFalse(user.synthetic());
        Assert.assertTrue(expired.synthetic());
        Assert.assertSame(record, expired.record());
        Assert.assertEquals(17L, expired.commitAttemptTimestampMillis());
    }

    @Test
    public void noopPublisherValidatesButNeverRetainsOrConsumesCapacity() {
        ByteArrayCommandRecord record = ByteArrayCommandRecord.copyOf(new byte[]{'S', 'E', 'T'});
        DbCommitPublisher publisher = DbCommitPublisher.NOOP;

        Assert.assertFalse(publisher.enabled());
        Assert.assertTrue(publisher.available());
        DbCommitReservation reservation = publisher.reserve(0, DbCommitKind.USER, record, 4L, 12L);
        Assert.assertSame(DbCommitReservation.NOOP, reservation);
        Assert.assertTrue(reservation.noop());
        Assert.assertEquals(0L, reservation.reservedMemoryBytes());
        Assert.assertEquals(0L, publisher.publish(reservation));
        publisher.failAfterCommit(reservation);
        reservation.close();
        reservation.close();

        Assert.assertThrows(IllegalArgumentException.class,
                () -> publisher.reserve(-1, DbCommitKind.USER, record, 0L, 0L));
        Assert.assertThrows(NullPointerException.class,
                () -> publisher.reserve(0, null, record, 0L, 0L));
        Assert.assertThrows(NullPointerException.class,
                () -> publisher.reserve(0, DbCommitKind.USER, null, 0L, 0L));
        record.close();
    }

    @Test
    public void unavailableExceptionUsesTheStableBusyReply() {
        Assert.assertEquals("BUSY commit stream unavailable", new DbCommitStreamUnavailableException().getMessage());
    }

    private static DbCommitEvent event(DbCommitKind kind, CommandRecordView record) {
        return new DbCommitEvent() {
            @Override
            public long sequence() {
                return 3L;
            }

            @Override
            public int dbIndex() {
                return 2;
            }

            @Override
            public DbCommitKind kind() {
                return kind;
            }

            @Override
            public CommandRecordView record() {
                return record;
            }

            @Override
            public long committedMemoryDelta() {
                return -5L;
            }

            @Override
            public long commitAttemptTimestampMillis() {
                return 17L;
            }
        };
    }
}
