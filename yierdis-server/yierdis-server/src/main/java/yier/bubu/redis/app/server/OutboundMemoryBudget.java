package yier.bubu.redis.app.server;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 统一管理全局回复额度、连接账户和 FIFO 等待者。
 */
public final class OutboundMemoryBudget implements AutoCloseable {
    private final Object lock = new Object();
    private final long capacityBytes;
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final Map<OutboundConnectionMemory, Boolean> connections = new IdentityHashMap<>();
    private final Map<OutboundConnectionMemory, Waiter> waitersByConnection = new IdentityHashMap<>();

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

    public OutboundConnectionMemory openConnection(long connectionCapacityBytes) {
        if (connectionCapacityBytes <= 0L || connectionCapacityBytes > capacityBytes) {
            throw new IllegalArgumentException("connectionCapacityBytes must be in range 1..capacityBytes");
        }
        OutboundConnectionMemory connection = new OutboundConnectionMemory(this, connectionCapacityBytes);
        synchronized (lock) {
            if (closed) {
                connection.markClosed();
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

    Optional<OutboundMemoryLease> reserve(
            OutboundConnectionMemory connection,
            long bytes,
            long singleReplyLimitBytes
    ) {
        validateReservationArguments(bytes, singleReplyLimitBytes);
        Objects.requireNonNull(connection, "connection");

        Runnable callback;
        OutboundMemoryLease lease;
        synchronized (lock) {
            requireOwner(connection);
            if (closed || connection.closed()) {
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
                capacityRejectedReservations = saturatedAdd(capacityRejectedReservations, 1L);
                return Optional.empty();
            }
            if (waiter != null) {
                removeWaiterLocked(waiter);
            }

            reserveLocked(connection, bytes);
            lease = new OutboundMemoryLease(this, connection, bytes);
            callback = grantOneWaiterLocked();
        }
        invokeCallback(callback);
        return Optional.of(lease);
    }

    boolean awaitCapacity(
            OutboundConnectionMemory connection,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        validateReservationArguments(bytes, singleReplyLimitBytes);
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(callback, "callback");

        Runnable grantedCallback;
        synchronized (lock) {
            requireOwner(connection);
            if (closed || connection.closed()) {
                return false;
            }
            requireAttached(connection);
            if (!fitsSingle(bytes, singleReplyLimitBytes) || bytes > connection.capacityBytes() || bytes > capacityBytes) {
                capacityRejectedReservations = saturatedAdd(capacityRejectedReservations, 1L);
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

    void cancelWaiter(OutboundConnectionMemory connection) {
        Objects.requireNonNull(connection, "connection");
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

    boolean convertToAllocated(OutboundMemoryLease lease, long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        Objects.requireNonNull(lease, "lease");
        synchronized (lock) {
            if (lease.closed()) {
                return false;
            }
            requireAttached(lease.connection());
            if (bytes > lease.reservedBytes() - lease.allocatedBytes()) {
                return false;
            }
            lease.addAllocated(bytes);
            lease.connection().addAllocated(bytes);
            allocatedBytes += bytes;
            peakAllocatedBytes = Math.max(peakAllocatedBytes, allocatedBytes);
            return true;
        }
    }

    boolean expandLease(OutboundMemoryLease lease, long bytes, long singleReplyLimitBytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        if (singleReplyLimitBytes <= 0L) {
            throw new IllegalArgumentException("singleReplyLimitBytes must be > 0");
        }
        Objects.requireNonNull(lease, "lease");
        if (bytes == 0L) {
            return !lease.closed();
        }

        Runnable callback;
        synchronized (lock) {
            if (lease.closed() || closed || lease.connection().closed()) {
                return false;
            }
            OutboundConnectionMemory connection = lease.connection();
            requireAttached(connection);
            Waiter waiter = waitersByConnection.get(connection);
            if (waiter != null) {
                if (!waiter.granted || waiters.peekFirst() != waiter
                        || waiter.lease != lease
                        || waiter.bytes != bytes || waiter.singleReplyLimitBytes != singleReplyLimitBytes) {
                    capacityRejectedReservations = saturatedAdd(capacityRejectedReservations, 1L);
                    return false;
                }
            } else if (hasGrantedWaiterLocked()) {
                return false;
            }
            if (!fitsWithin(lease.reservedBytes(), bytes, singleReplyLimitBytes)
                    || !fitsConnection(connection, bytes)
                    || !fitsGlobal(bytes)) {
                capacityRejectedReservations = saturatedAdd(capacityRejectedReservations, 1L);
                return false;
            }
            if (waiter != null) {
                removeWaiterLocked(waiter);
            }
            connection.extendReservation(bytes);
            lease.addReservedBytes(bytes);
            reservedBytes += bytes;
            peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
            callback = grantOneWaiterLocked();
        }
        invokeCallback(callback);
        return true;
    }

    boolean awaitLeaseExpansion(
            OutboundMemoryLease lease,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        validateReservationArguments(bytes, singleReplyLimitBytes);
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(callback, "callback");

        Runnable grantedCallback;
        synchronized (lock) {
            OutboundConnectionMemory connection = lease.connection();
            requireOwner(connection);
            if (lease.closed() || closed || connection.closed()) {
                return false;
            }
            requireAttached(connection);
            if (!fitsWithin(lease.reservedBytes(), bytes, singleReplyLimitBytes)
                    || !fitsWithin(lease.reservedBytes(), bytes, connection.capacityBytes())
                    || !fitsWithin(lease.reservedBytes(), bytes, capacityBytes)) {
                capacityRejectedReservations = saturatedAdd(capacityRejectedReservations, 1L);
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

    void cancelLeaseExpansionWaiter(OutboundMemoryLease lease) {
        Objects.requireNonNull(lease, "lease");
        Runnable callback;
        synchronized (lock) {
            OutboundConnectionMemory connection = lease.connection();
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

    void releaseAllocated(OutboundMemoryLease lease, long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        Objects.requireNonNull(lease, "lease");
        synchronized (lock) {
            if (lease.closed()) {
                return;
            }
            requireAttached(lease.connection());
            if (bytes > lease.allocatedBytes()) {
                throw new IllegalArgumentException("allocated release exceeds lease allocation");
            }
            lease.releaseAllocatedBytes(bytes);
            lease.connection().releaseAllocated(bytes);
            if (bytes > allocatedBytes) {
                throw new IllegalStateException("outbound budget allocation underflow");
            }
            allocatedBytes -= bytes;
        }
    }

    void closeLease(OutboundMemoryLease lease) {
        Objects.requireNonNull(lease, "lease");
        Runnable callback;
        synchronized (lock) {
            if (lease.closed()) {
                return;
            }
            OutboundConnectionMemory connection = lease.connection();
            requireAttached(connection);
            removeWaiterForLeaseLocked(lease);
            long allocated = lease.allocatedBytes();
            if (allocated > 0L) {
                lease.releaseAllocatedBytes(allocated);
                connection.releaseAllocated(allocated);
                if (allocated > allocatedBytes) {
                    throw new IllegalStateException("outbound budget allocation underflow");
                }
                allocatedBytes -= allocated;
            }
            long reserved = lease.reservedBytes();
            connection.releaseReservation(reserved);
            if (reserved > reservedBytes || activeSlots <= 0L) {
                throw new IllegalStateException("outbound budget reservation underflow");
            }
            reservedBytes -= reserved;
            activeSlots--;
            lease.markClosed();
            removeClosedEmptyConnectionLocked(connection);
            callback = closed ? null : grantOneWaiterLocked();
        }
        invokeCallback(callback);
    }

    void closeConnection(OutboundConnectionMemory connection) {
        Objects.requireNonNull(connection, "connection");
        Runnable callback;
        synchronized (lock) {
            if (!connections.containsKey(connection)) {
                return;
            }
            if (!connection.closed()) {
                connection.markClosed();
            }
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
            java.util.Iterator<Map.Entry<OutboundConnectionMemory, Boolean>> iterator = connections.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<OutboundConnectionMemory, Boolean> entry = iterator.next();
                OutboundConnectionMemory connection = entry.getKey();
                connection.markClosed();
                if (connection.activeSlots() == 0L) {
                    if (Boolean.TRUE.equals(entry.getValue())) {
                        activeConnections--;
                    }
                    iterator.remove();
                }
            }
        }
    }

    private void reserveLocked(OutboundConnectionMemory connection, long bytes) {
        connection.addReservation(bytes);
        reservedBytes += bytes;
        activeSlots++;
        peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
    }

    private Runnable grantOneWaiterLocked() {
        while (!waiters.isEmpty()) {
            Waiter waiter = waiters.peekFirst();
            OutboundConnectionMemory connection = waiter.connection;
            if (connection.closed() || !connections.containsKey(connection)
                    || (waiter.lease != null && waiter.lease.closed())) {
                removeWaiterLocked(waiter);
                continue;
            }
            boolean fitsSingle = waiter.lease == null
                    ? fitsSingle(waiter.bytes, waiter.singleReplyLimitBytes)
                    : fitsWithin(waiter.lease.reservedBytes(), waiter.bytes, waiter.singleReplyLimitBytes);
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

    private void removeWaiterForLeaseLocked(OutboundMemoryLease lease) {
        Waiter waiter = waitersByConnection.get(lease.connection());
        if (waiter != null && waiter.lease == lease) {
            removeWaiterLocked(waiter);
        }
    }

    private void removeClosedEmptyConnectionLocked(OutboundConnectionMemory connection) {
        if (connection.closed() && connection.activeSlots() == 0L) {
            Boolean counted = connections.remove(connection);
            if (Boolean.TRUE.equals(counted)) {
                activeConnections--;
            }
        }
    }

    private void requireAttached(OutboundConnectionMemory connection) {
        if (!connections.containsKey(connection)) {
            throw new IllegalStateException("connection memory account is not attached to this budget");
        }
    }

    private void requireOwner(OutboundConnectionMemory connection) {
        if (connection.budget() != this) {
            throw new IllegalStateException("connection memory account belongs to another budget");
        }
    }

    private boolean fitsSingle(long bytes, long singleReplyLimitBytes) {
        return bytes <= singleReplyLimitBytes;
    }

    private boolean fitsConnection(OutboundConnectionMemory connection, long bytes) {
        return fitsWithin(connection.reservedBytes(), bytes, connection.capacityBytes());
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

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
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

    private static final class Waiter {
        private final OutboundConnectionMemory connection;
        private final OutboundMemoryLease lease;
        private final long bytes;
        private final long singleReplyLimitBytes;
        private final Runnable callback;
        private boolean granted;

        private Waiter(
                OutboundConnectionMemory connection,
                OutboundMemoryLease lease,
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
