package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.YierdisCommandException;

public final class YierdisDbMemoryLedger implements MemoryLedger {
    private final long limitBytes;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final Runnable cleanupExpired;
    private final LongConsumer evictUntilUnder;
    private final LongSupplier usedBytesForMaxmemory;
    private final Supplier<MaxmemoryCoordinator> maxmemoryCoordinatorSupplier;
    private final Supplier<MaxmemoryParticipant> maxmemoryParticipantSupplier;

    private long usedBytes;
    private long reservedBytes;

    public YierdisDbMemoryLedger(
            long limitBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            Runnable cleanupExpired,
            LongConsumer evictUntilUnder,
            LongSupplier usedBytesForMaxmemory,
            Supplier<MaxmemoryCoordinator> maxmemoryCoordinatorSupplier,
            Supplier<MaxmemoryParticipant> maxmemoryParticipantSupplier
    ) {
        this.limitBytes = Math.max(0L, limitBytes);
        this.maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        this.cleanupExpired = Objects.requireNonNull(cleanupExpired, "cleanupExpired");
        this.evictUntilUnder = Objects.requireNonNull(evictUntilUnder, "evictUntilUnder");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
        this.maxmemoryCoordinatorSupplier = Objects.requireNonNull(maxmemoryCoordinatorSupplier, "maxmemoryCoordinatorSupplier");
        this.maxmemoryParticipantSupplier = Objects.requireNonNull(maxmemoryParticipantSupplier, "maxmemoryParticipantSupplier");
    }

    @Override
    public long limitBytes() {
        return limitBytes;
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long reservedBytes() {
        return reservedBytes;
    }

    @Override
    public boolean maxmemoryEnabled() {
        return limitBytes > 0L || maxmemoryCoordinatorSupplier.get() != null;
    }

    @Override
    public MemoryReservation reserve(long estimatedExtraBytes) {
        if (estimatedExtraBytes < 0) {
            throw new IllegalArgumentException("estimatedExtraBytes must be >= 0");
        }
        long nextReservedBytes = estimatedExtraBytes == 0L
                ? reservedBytes
                : checkedReservationTotal(reservedBytes, estimatedExtraBytes);

        MaxmemoryCoordinator coordinator = maxmemoryCoordinatorSupplier.get();
        if (coordinator != null) {
            try {
                // coordinator 接管跨 DB/全局预算判定；本地 ledger 仍保留 reservation，保证 commit/rollback 对账。
                coordinator.prepareWrite(maxmemoryParticipantSupplier.get(), estimatedExtraBytes);
            } catch (YierdisCommandException e) {
                throw new MemoryLedgerOutOfMemoryException();
            }
            if (estimatedExtraBytes == 0) {
                return NoopReservation.INSTANCE;
            }
            reservedBytes = nextReservedBytes;
            return new ReservationToken(this, estimatedExtraBytes);
        }

        enforceLocalLimit(estimatedExtraBytes, true);

        if (estimatedExtraBytes == 0) {
            return NoopReservation.INSTANCE;
        }
        reservedBytes = nextReservedBytes;
        return new ReservationToken(this, estimatedExtraBytes);
    }

    @Override
    public void reconcile(MemoryReservation reservation, long requiredBytes) {
        if (requiredBytes < 0) {
            throw new IllegalArgumentException("requiredBytes must be >= 0");
        }
        ReservationToken token = ReservationToken.validate(reservation, this);
        long reserved = token == null ? 0L : token.reservedBytes;
        if (requiredBytes > reserved) {
            throw new IllegalStateException(
                    "prepared mutation exceeded its reservation: required=" + requiredBytes + ", reserved=" + reserved
            );
        }
        if (token != null && requiredBytes < reserved) {
            reservedBytes -= reserved - requiredBytes;
            token.reservedBytes = requiredBytes;
        }
    }

    @Override
    public MemoryReservation beginReclamation() {
        return new ReservationToken(this, 0L);
    }

    @Override
    public void commit(MemoryReservation reservation, long actualDeltaBytes) {
        ReservationToken token = ReservationToken.validate(reservation, this);
        long nextUsedBytes = checkedCommittedUsage(usedBytes, actualDeltaBytes);
        if (token != null) {
            token.finish();
            reservedBytes -= token.reservedBytes;
            if (reservedBytes < 0) {
                throw new IllegalStateException("reservedBytes underflow");
            }
        }

        usedBytes = nextUsedBytes;
    }

    @Override
    public void rollback(MemoryReservation reservation) {
        ReservationToken token = ReservationToken.validate(reservation, this);
        if (token == null) {
            return;
        }
        token.finish();
        reservedBytes -= token.reservedBytes;
        if (reservedBytes < 0) {
            throw new IllegalStateException("reservedBytes underflow");
        }
    }

    public void resetUsage() {
        usedBytes = 0;
        reservedBytes = 0;
    }

    public void enforceLocalMaintenance() {
        enforceLocalLimit(0L, false);
    }

    private void enforceLocalLimit(long estimatedExtraBytes, boolean cleanupBeforeAdmission) {
        if (limitBytes <= 0L) {
            return;
        }
        if (cleanupBeforeAdmission) {
            cleanupExpired.run();
        }
        if (estimatedExtraBytes > 0L && estimatedExtraBytes > limitBytes) {
            throw new MemoryLedgerOutOfMemoryException();
        }

        long limit = limitBytes - estimatedExtraBytes;
        if (limit < 0L) {
            limit = 0L;
        }
        if (usedBytesForMaxmemory.getAsLong() <= limit) {
            return;
        }

        evictUntilUnder.accept(limit);
        if (usedBytesForMaxmemory.getAsLong() <= limit) {
            return;
        }
        if (estimatedExtraBytes > 0L) {
            throw new MemoryLedgerOutOfMemoryException();
        }
    }

    private static long checkedReservationTotal(long current, long increment) {
        try {
            return Math.addExact(current, increment);
        } catch (ArithmeticException overflow) {
            throw new MemoryLedgerOutOfMemoryException();
        }
    }

    private static long checkedCommittedUsage(long current, long delta) {
        final long next;
        try {
            next = Math.addExact(current, delta);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("usedBytes overflow", overflow);
        }
        if (next < 0L) {
            throw new IllegalStateException("usedBytes underflow");
        }
        return next;
    }

    private enum NoopReservation implements MemoryReservation {
        INSTANCE;

        @Override
        public long reservedBytes() {
            return 0;
        }
    }

    private static final class ReservationToken implements MemoryReservation {
        private final YierdisDbMemoryLedger owner;
        private long reservedBytes;
        private boolean finished;

        private ReservationToken(YierdisDbMemoryLedger owner, long reservedBytes) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.reservedBytes = reservedBytes;
        }

        @Override
        public long reservedBytes() {
            return reservedBytes;
        }

        private void finish() {
            if (finished) {
                throw new IllegalStateException("reservation already finished");
            }
            finished = true;
        }

        private static ReservationToken validate(MemoryReservation reservation, YierdisDbMemoryLedger expectedOwner) {
            if (reservation == null) {
                return null;
            }
            if (reservation instanceof NoopReservation) {
                return null;
            }
            if (!(reservation instanceof ReservationToken token)) {
                throw new IllegalArgumentException("unknown reservation type: " + reservation.getClass().getName());
            }
            if (token.owner != expectedOwner) {
                throw new IllegalArgumentException("reservation does not belong to this ledger");
            }
            return token;
        }
    }
}
