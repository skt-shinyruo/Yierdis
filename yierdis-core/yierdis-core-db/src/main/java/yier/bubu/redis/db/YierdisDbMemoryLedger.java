package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.MemoryLedger;
import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.db.memory.MemoryReservation;
import yier.bubu.redis.ops.MaxmemoryCoordinator;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.ops.YierdisCommandException;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

final class YierdisDbMemoryLedger implements MemoryLedger {
    private final long limitBytes;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final Runnable cleanupExpired;
    private final LongConsumer evictUntilUnder;
    private final LongSupplier usedBytesForMaxmemory;
    private final Supplier<MaxmemoryCoordinator> maxmemoryCoordinatorSupplier;

    private long usedBytes;
    private long reservedBytes;

    YierdisDbMemoryLedger(
            long limitBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            Runnable cleanupExpired,
            LongConsumer evictUntilUnder,
            LongSupplier usedBytesForMaxmemory,
            Supplier<MaxmemoryCoordinator> maxmemoryCoordinatorSupplier
    ) {
        this.limitBytes = Math.max(0L, limitBytes);
        this.maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        this.cleanupExpired = Objects.requireNonNull(cleanupExpired, "cleanupExpired");
        this.evictUntilUnder = Objects.requireNonNull(evictUntilUnder, "evictUntilUnder");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
        this.maxmemoryCoordinatorSupplier = Objects.requireNonNull(maxmemoryCoordinatorSupplier, "maxmemoryCoordinatorSupplier");
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
    public MemoryReservation reserve(long estimatedExtraBytes) {
        if (estimatedExtraBytes < 0) {
            throw new IllegalArgumentException("estimatedExtraBytes must be >= 0");
        }

        MaxmemoryCoordinator coordinator = maxmemoryCoordinatorSupplier.get();
        if (coordinator != null) {
            try {
                coordinator.prepareWrite(estimatedExtraBytes);
            } catch (YierdisCommandException e) {
                throw new MemoryLedgerOutOfMemoryException();
            }
            if (estimatedExtraBytes == 0) {
                return NoopReservation.INSTANCE;
            }
            reservedBytes += estimatedExtraBytes;
            return new ReservationToken(this, estimatedExtraBytes);
        }

        if (limitBytes > 0) {
            cleanupExpired.run();

            if (estimatedExtraBytes > 0 && estimatedExtraBytes > limitBytes) {
                throw new MemoryLedgerOutOfMemoryException();
            }

            long limit = limitBytes - estimatedExtraBytes;
            if (limit < 0) {
                limit = 0;
            }
            if (usedBytesForMaxmemory.getAsLong() > limit) {
                if (maxmemoryPolicy == MaxmemoryPolicy.NOEVICTION) {
                    if (estimatedExtraBytes > 0) {
                        throw new MemoryLedgerOutOfMemoryException();
                    }
                    return NoopReservation.INSTANCE;
                }
                evictUntilUnder.accept(limit);
                if (usedBytesForMaxmemory.getAsLong() > limit) {
                    if (estimatedExtraBytes > 0) {
                        throw new MemoryLedgerOutOfMemoryException();
                    }
                    return NoopReservation.INSTANCE;
                }
            }
        }

        if (estimatedExtraBytes == 0) {
            return NoopReservation.INSTANCE;
        }
        reservedBytes += estimatedExtraBytes;
        return new ReservationToken(this, estimatedExtraBytes);
    }

    @Override
    public void commit(MemoryReservation reservation, long actualDeltaBytes) {
        ReservationToken token = ReservationToken.validate(reservation, this);
        if (token != null) {
            token.finish();
            reservedBytes -= token.reservedBytes;
            if (reservedBytes < 0) {
                throw new IllegalStateException("reservedBytes underflow");
            }
        }

        if (actualDeltaBytes == 0) {
            return;
        }
        usedBytes += actualDeltaBytes;
        if (usedBytes < 0) {
            throw new IllegalStateException("usedBytes underflow");
        }
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

    void resetUsage() {
        usedBytes = 0;
        reservedBytes = 0;
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
        private final long reservedBytes;
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
