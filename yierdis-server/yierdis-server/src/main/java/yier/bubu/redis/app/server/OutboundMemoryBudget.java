package yier.bubu.redis.app.server;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 统一管理全局回复额度、连接账户和 FIFO 等待者。
 *
 * <p>{@link Connection} 与 {@link Lease} 是预算拥有的嵌套句柄：对外只暴露委托入口，
 * 句柄账目由预算在锁内直接改写，不存在反向的句柄 mutator。</p>
 */
public final class OutboundMemoryBudget implements AutoCloseable {
    private final Object lock = new Object();
    private final long capacityBytes;
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final Map<Connection, Boolean> connections = new IdentityHashMap<>();
    private final Map<Connection, Waiter> waitersByConnection = new IdentityHashMap<>();

    private long reservedBytes;
    private long allocatedBytes;
    private long peakReservedBytes;
    private long peakAllocatedBytes;
    private long capacityRejectedReservations;
    private long activeSlots;
    private int activeConnections;
    private boolean closed;

    public OutboundMemoryBudget(long capacityBytes) {
        if (capacityBytes <= 0L) {
            throw new IllegalArgumentException("capacityBytes must be > 0");
        }
        this.capacityBytes = capacityBytes;
    }

    public Connection openConnection(long connectionCapacityBytes) {
        if (connectionCapacityBytes <= 0L || connectionCapacityBytes > capacityBytes) {
            throw new IllegalArgumentException("connectionCapacityBytes must be in range 1..capacityBytes");
        }
        Connection connection = new Connection(connectionCapacityBytes);
        synchronized (lock) {
            if (closed) {
                connection.closed = true;
                return connection;
            }
            connections.put(connection, Boolean.TRUE);
            activeConnections++;
        }
        return connection;
    }

    public OutboundMemoryBudgetStats stats() {
        synchronized (lock) {
            return new OutboundMemoryBudgetStats(
                    capacityBytes,
                    reservedBytes,
                    allocatedBytes,
                    peakReservedBytes,
                    peakAllocatedBytes,
                    capacityRejectedReservations,
                    waitersByConnection.size(),
                    activeConnections,
                    activeSlots,
                    closed
            );
        }
    }

    private Optional<Lease> reserve(Connection connection, long bytes, long singleReplyLimitBytes) {
        Runnable callback;
        Lease lease;
        synchronized (lock) {
            if (closed || connection.closed) {
                return Optional.empty();
            }
            requireAttached(connection);
            Waiter waiter = waitersByConnection.get(connection);
            if (waiter != null) {
                if (!waiter.granted || waiters.peekFirst() != waiter
                        || waiter.lease != null
                        || waiter.bytes != bytes || waiter.singleReplyLimitBytes != singleReplyLimitBytes) {
                    return Optional.empty();
                }
            } else if (hasGrantedWaiterLocked()) {
                return Optional.empty();
            }

            if (!fitsSingle(bytes, singleReplyLimitBytes) || !fitsConnection(connection, bytes) || !fitsGlobal(bytes)) {
                capacityRejectedReservations = addSaturating(capacityRejectedReservations, 1L);
                return Optional.empty();
            }
            if (waiter != null) {
                removeWaiterLocked(waiter);
            }

            reserveLocked(connection, bytes);
            lease = new Lease(connection, bytes);
            callback = grantOneWaiterLocked();
        }
        invokeCallback(callback);
        return Optional.of(lease);
    }

    private boolean awaitCapacity(
            Connection connection,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        Runnable grantedCallback;
        synchronized (lock) {
            if (closed || connection.closed) {
                return false;
            }
            requireAttached(connection);
            if (!fitsSingle(bytes, singleReplyLimitBytes) || bytes > connection.capacityBytes || bytes > capacityBytes) {
                capacityRejectedReservations = addSaturating(capacityRejectedReservations, 1L);
                return false;
            }

            Waiter existing = waitersByConnection.get(connection);
            if (existing != null) {
                return existing.lease == null
                        && existing.bytes == bytes
                        && existing.singleReplyLimitBytes == singleReplyLimitBytes;
            }

            Waiter waiter = new Waiter(connection, null, bytes, singleReplyLimitBytes, callback);
            waiters.addLast(waiter);
            waitersByConnection.put(connection, waiter);
            grantedCallback = grantOneWaiterLocked();
        }
        invokeCallback(grantedCallback);
        return true;
    }

    private void cancelWaiter(Connection connection) {
        Runnable callback;
        synchronized (lock) {
            if (!connections.containsKey(connection)) {
                return;
            }
            removeWaiterLocked(waitersByConnection.get(connection));
            callback = closed ? null : grantOneWaiterLocked();
        }
        invokeCallback(callback);
    }

    private boolean convertToAllocated(Lease lease, long bytes) {
        synchronized (lock) {
            if (lease.closed) {
                return false;
            }
            requireAttached(lease.connection);
            if (bytes > lease.reservedBytes - lease.allocatedBytes) {
                return false;
            }
            lease.allocatedBytes += bytes;
            lease.connection.allocatedBytes += bytes;
            allocatedBytes += bytes;
            peakAllocatedBytes = Math.max(peakAllocatedBytes, allocatedBytes);
            return true;
        }
    }

    private boolean expandLease(Lease lease, long bytes, long singleReplyLimitBytes) {
        if (bytes == 0L) {
            return !lease.closed;
        }

        Runnable callback;
        synchronized (lock) {
            if (lease.closed || closed || lease.connection.closed) {
                return false;
            }
            Connection connection = lease.connection;
            requireAttached(connection);
            Waiter waiter = waitersByConnection.get(connection);
            if (waiter != null) {
                if (!waiter.granted || waiters.peekFirst() != waiter
                        || waiter.lease != lease
                        || waiter.bytes != bytes || waiter.singleReplyLimitBytes != singleReplyLimitBytes) {
                    capacityRejectedReservations = addSaturating(capacityRejectedReservations, 1L);
                    return false;
                }
            } else if (hasGrantedWaiterLocked()) {
                return false;
            }
            if (!fitsWithin(lease.reservedBytes, bytes, singleReplyLimitBytes)
                    || !fitsConnection(connection, bytes)
                    || !fitsGlobal(bytes)) {
                capacityRejectedReservations = addSaturating(capacityRejectedReservations, 1L);
                return false;
            }
            if (waiter != null) {
                removeWaiterLocked(waiter);
            }
            connection.reservedBytes += bytes;
            lease.reservedBytes += bytes;
            reservedBytes += bytes;
            peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
            callback = grantOneWaiterLocked();
        }
        invokeCallback(callback);
        return true;
    }

    private boolean awaitLeaseExpansion(
            Lease lease,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        Runnable grantedCallback;
        synchronized (lock) {
            Connection connection = lease.connection;
            if (lease.closed || closed || connection.closed) {
                return false;
            }
            requireAttached(connection);
            if (!fitsWithin(lease.reservedBytes, bytes, singleReplyLimitBytes)
                    || !fitsWithin(lease.reservedBytes, bytes, connection.capacityBytes)
                    || !fitsWithin(lease.reservedBytes, bytes, capacityBytes)) {
                capacityRejectedReservations = addSaturating(capacityRejectedReservations, 1L);
                return false;
            }

            Waiter existing = waitersByConnection.get(connection);
            if (existing != null) {
                return existing.lease == lease
                        && existing.bytes == bytes
                        && existing.singleReplyLimitBytes == singleReplyLimitBytes;
            }

            Waiter waiter = new Waiter(connection, lease, bytes, singleReplyLimitBytes, callback);
            waiters.addLast(waiter);
            waitersByConnection.put(connection, waiter);
            grantedCallback = grantOneWaiterLocked();
        }
        invokeCallback(grantedCallback);
        return true;
    }

    private void cancelLeaseExpansionWaiter(Lease lease) {
        Runnable callback;
        synchronized (lock) {
            Connection connection = lease.connection;
            if (!connections.containsKey(connection)) {
                return;
            }
            Waiter waiter = waitersByConnection.get(connection);
            if (waiter == null || waiter.lease != lease) {
                return;
            }
            removeWaiterLocked(waiter);
            callback = closed ? null : grantOneWaiterLocked();
        }
        invokeCallback(callback);
    }

    private void releaseAllocated(Lease lease, long bytes) {
        synchronized (lock) {
            if (lease.closed) {
                return;
            }
            requireAttached(lease.connection);
            if (bytes > lease.allocatedBytes) {
                throw new IllegalArgumentException("allocated release exceeds lease allocation");
            }
            lease.allocatedBytes -= bytes;
            releaseConnectionAllocatedLocked(lease.connection, bytes);
            if (bytes > allocatedBytes) {
                throw new IllegalStateException("outbound budget allocation underflow");
            }
            allocatedBytes -= bytes;
        }
    }

    private void closeLease(Lease lease) {
        Runnable callback;
        synchronized (lock) {
            if (lease.closed) {
                return;
            }
            Connection connection = lease.connection;
            requireAttached(connection);
            removeWaiterForLeaseLocked(lease);
            long allocated = lease.allocatedBytes;
            if (allocated > 0L) {
                lease.allocatedBytes = 0L;
                releaseConnectionAllocatedLocked(connection, allocated);
                if (allocated > allocatedBytes) {
                    throw new IllegalStateException("outbound budget allocation underflow");
                }
                allocatedBytes -= allocated;
            }
            long reserved = lease.reservedBytes;
            if (reserved > connection.reservedBytes || connection.activeSlots <= 0L) {
                throw new IllegalStateException("outbound connection reservation underflow");
            }
            connection.reservedBytes -= reserved;
            connection.activeSlots--;
            if (reserved > reservedBytes || activeSlots <= 0L) {
                throw new IllegalStateException("outbound budget reservation underflow");
            }
            reservedBytes -= reserved;
            activeSlots--;
            lease.closed = true;
            removeClosedEmptyConnectionLocked(connection);
            callback = closed ? null : grantOneWaiterLocked();
        }
        invokeCallback(callback);
    }

    private void closeConnection(Connection connection) {
        Runnable callback;
        synchronized (lock) {
            if (!connections.containsKey(connection)) {
                return;
            }
            connection.closed = true;
            if (Boolean.TRUE.equals(connections.put(connection, Boolean.FALSE))) {
                activeConnections--;
            }
            removeWaiterLocked(waitersByConnection.get(connection));
            removeClosedEmptyConnectionLocked(connection);
            callback = closed ? null : grantOneWaiterLocked();
        }
        invokeCallback(callback);
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            waiters.clear();
            waitersByConnection.clear();
            Iterator<Map.Entry<Connection, Boolean>> iterator = connections.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Connection, Boolean> entry = iterator.next();
                Connection connection = entry.getKey();
                connection.closed = true;
                if (connection.activeSlots == 0L) {
                    if (Boolean.TRUE.equals(entry.getValue())) {
                        activeConnections--;
                    }
                    iterator.remove();
                }
            }
        }
    }

    private void reserveLocked(Connection connection, long bytes) {
        connection.reservedBytes += bytes;
        connection.activeSlots++;
        reservedBytes += bytes;
        activeSlots++;
        peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
    }

    private static void releaseConnectionAllocatedLocked(Connection connection, long bytes) {
        if (bytes > connection.allocatedBytes) {
            throw new IllegalStateException("outbound connection allocation underflow");
        }
        connection.allocatedBytes -= bytes;
    }

    private Runnable grantOneWaiterLocked() {
        while (!waiters.isEmpty()) {
            Waiter waiter = waiters.peekFirst();
            Connection connection = waiter.connection;
            if (connection.closed || !connections.containsKey(connection)
                    || (waiter.lease != null && waiter.lease.closed)) {
                removeWaiterLocked(waiter);
                continue;
            }
            boolean fitsSingle = waiter.lease == null
                    ? fitsSingle(waiter.bytes, waiter.singleReplyLimitBytes)
                    : fitsWithin(waiter.lease.reservedBytes, waiter.bytes, waiter.singleReplyLimitBytes);
            if (waiter.granted || !fitsSingle || !fitsConnection(connection, waiter.bytes) || !fitsGlobal(waiter.bytes)) {
                return null;
            }
            waiter.granted = true;
            return () -> invokeGrantedWaiter(waiter);
        }
        return null;
    }

    private boolean hasGrantedWaiterLocked() {
        Waiter head = waiters.peekFirst();
        return head != null && head.granted;
    }

    private void invokeGrantedWaiter(Waiter waiter) {
        try {
            waiter.callback.run();
        } catch (RuntimeException ignored) {
            Runnable callback = null;
            synchronized (lock) {
                if (waitersByConnection.get(waiter.connection) == waiter && waiter.granted) {
                    removeWaiterLocked(waiter);
                    callback = closed ? null : grantOneWaiterLocked();
                }
            }
            invokeCallback(callback);
        }
    }

    private void removeWaiterLocked(Waiter waiter) {
        if (waiter == null) {
            return;
        }
        if (waitersByConnection.remove(waiter.connection, waiter)) {
            waiters.remove(waiter);
        }
    }

    private void removeWaiterForLeaseLocked(Lease lease) {
        Waiter waiter = waitersByConnection.get(lease.connection);
        if (waiter != null && waiter.lease == lease) {
            removeWaiterLocked(waiter);
        }
    }

    private void removeClosedEmptyConnectionLocked(Connection connection) {
        if (connection.closed && connection.activeSlots == 0L) {
            Boolean counted = connections.remove(connection);
            if (Boolean.TRUE.equals(counted)) {
                activeConnections--;
            }
        }
    }

    private void requireAttached(Connection connection) {
        if (!connections.containsKey(connection)) {
            throw new IllegalStateException("connection memory account is not attached to this budget");
        }
    }

    private boolean fitsSingle(long bytes, long singleReplyLimitBytes) {
        return bytes <= singleReplyLimitBytes;
    }

    private boolean fitsConnection(Connection connection, long bytes) {
        return fitsWithin(connection.reservedBytes, bytes, connection.capacityBytes);
    }

    private boolean fitsGlobal(long bytes) {
        return fitsWithin(reservedBytes, bytes, capacityBytes);
    }

    private static boolean fitsWithin(long current, long increment, long limit) {
        return current >= 0L && increment >= 0L && increment <= limit && current <= limit - increment;
    }

    private static void validateReservationArguments(long bytes, long singleReplyLimitBytes) {
        if (bytes <= 0L) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        if (singleReplyLimitBytes <= 0L) {
            throw new IllegalArgumentException("singleReplyLimitBytes must be > 0");
        }
    }

    private static void invokeCallback(Runnable callback) {
        if (callback == null) {
            return;
        }
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // 授予回调会自行撤销未消费的令牌；其他锁外通知失败不影响已完成的额度归还。
        }
    }

    /**
     * 一个连接的出站回复账户，不持有 Channel 等传输对象。
     */
    public final class Connection implements AutoCloseable {
        private final long capacityBytes;
        private volatile long reservedBytes;
        private volatile long allocatedBytes;
        private volatile long activeSlots;
        private volatile boolean closed;

        private Connection(long capacityBytes) {
            this.capacityBytes = capacityBytes;
        }

        public long capacityBytes() {
            return capacityBytes;
        }

        public long reservedBytes() {
            return reservedBytes;
        }

        public long allocatedBytes() {
            return allocatedBytes;
        }

        public long activeSlots() {
            return activeSlots;
        }

        public boolean closed() {
            return closed;
        }

        public Optional<Lease> reserve(long bytes, long singleReplyLimitBytes) {
            validateReservationArguments(bytes, singleReplyLimitBytes);
            return OutboundMemoryBudget.this.reserve(this, bytes, singleReplyLimitBytes);
        }

        public boolean awaitCapacity(long bytes, long singleReplyLimitBytes, Runnable callback) {
            validateReservationArguments(bytes, singleReplyLimitBytes);
            Objects.requireNonNull(callback, "callback");
            return OutboundMemoryBudget.this.awaitCapacity(this, bytes, singleReplyLimitBytes, callback);
        }

        public void cancelWaiter() {
            OutboundMemoryBudget.this.cancelWaiter(this);
        }

        @Override
        public void close() {
            closeConnection(this);
        }
    }

    /**
     * 一个顶层回复槽位的预留额度，可重复关闭。
     */
    public final class Lease implements AutoCloseable {
        private final Connection connection;
        private volatile long reservedBytes;
        private volatile long allocatedBytes;
        private volatile boolean closed;

        private Lease(Connection connection, long reservedBytes) {
            this.connection = connection;
            this.reservedBytes = reservedBytes;
        }

        public long reservedBytes() {
            return reservedBytes;
        }

        public long allocatedBytes() {
            return allocatedBytes;
        }

        public boolean closed() {
            return closed;
        }

        public boolean convertToAllocated(long bytes) {
            if (bytes < 0L) {
                throw new IllegalArgumentException("bytes must be non-negative");
            }
            return OutboundMemoryBudget.this.convertToAllocated(this, bytes);
        }

        public boolean tryReserveAdditional(long bytes, long singleReplyLimitBytes) {
            if (bytes < 0L) {
                throw new IllegalArgumentException("bytes must be non-negative");
            }
            if (singleReplyLimitBytes <= 0L) {
                throw new IllegalArgumentException("singleReplyLimitBytes must be > 0");
            }
            return expandLease(this, bytes, singleReplyLimitBytes);
        }

        public boolean awaitAdditionalCapacity(long bytes, long singleReplyLimitBytes, Runnable callback) {
            validateReservationArguments(bytes, singleReplyLimitBytes);
            Objects.requireNonNull(callback, "callback");
            return awaitLeaseExpansion(this, bytes, singleReplyLimitBytes, callback);
        }

        void cancelAdditionalCapacityWaiter() {
            cancelLeaseExpansionWaiter(this);
        }

        public void releaseAllocated(long bytes) {
            if (bytes < 0L) {
                throw new IllegalArgumentException("bytes must be non-negative");
            }
            OutboundMemoryBudget.this.releaseAllocated(this, bytes);
        }

        @Override
        public void close() {
            closeLease(this);
        }
    }

    private static final class Waiter {
        private final Connection connection;
        private final Lease lease;
        private final long bytes;
        private final long singleReplyLimitBytes;
        private final Runnable callback;
        private boolean granted;

        private Waiter(
                Connection connection,
                Lease lease,
                long bytes,
                long singleReplyLimitBytes,
                Runnable callback
        ) {
            this.connection = connection;
            this.lease = lease;
            this.bytes = bytes;
            this.singleReplyLimitBytes = singleReplyLimitBytes;
            this.callback = callback;
        }
    }
}
