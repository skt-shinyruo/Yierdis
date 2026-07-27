package yier.bubu.redis.protocol.resp.netty;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 服务器范围的入站内存预算。等待连接按队首顺序获得已预留的额度，再回到各自事件循环继续解码。
 */
public final class InboundMemoryBudget implements AutoCloseable {
    public enum ReservationResult {
        RESERVED,
        WAITING,
        REQUEST_LIMIT,
        CLOSED
    }

    private final Object lock = new Object();
    private final long capacityBytes;
    private final long highWatermarkBytes;
    private final long lowWatermarkBytes;
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final Map<InboundConnectionMemory, Waiter> waitersByConnection = new IdentityHashMap<>();
    private final Map<ConnectionMemoryAccount, Boolean> accounts = new IdentityHashMap<>();

    private long reservedBytes;
    private long peakReservedBytes;
    private long rejectedConnections;
    private long readCreditBytes;
    private long retainedInputCapacityBytes;
    private long consolidationBytes;
    private boolean backpressured;
    private boolean closed;

    public InboundMemoryBudget(long capacityBytes) {
        if (capacityBytes < 0L) {
            throw new IllegalArgumentException("capacityBytes must be non-negative");
        }
        this.capacityBytes = capacityBytes;
        this.highWatermarkBytes = highWatermark(capacityBytes);
        this.lowWatermarkBytes = capacityBytes / 2L;
    }

    public ReservationResult tryReserve(InboundConnectionMemory connection, long bytes) {
        return tryAdmit(connection, bytes, 0L);
    }

    public ReservationResult tryTransfer(
            InboundConnectionMemory connection,
            long newBytes,
            long inputCapacityReleasedAfterCopy
    ) {
        return tryAdmit(connection, newBytes, inputCapacityReleasedAfterCopy);
    }

    public void release(InboundConnectionMemory connection, long bytes) {
        Objects.requireNonNull(connection, "connection");
        release(connection.account(), bytes);
    }

    void release(ConnectionMemoryAccount account, long bytes) {
        Objects.requireNonNull(account, "account");
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }

        Granted granted;
        synchronized (lock) {
            if (!accounts.containsKey(account)) {
                throw new IllegalStateException("connection memory account is not attached to this budget");
            }
            account.releaseReserved(bytes);
            if (bytes > reservedBytes) {
                throw new IllegalStateException("inbound memory release exceeds reservation");
            }
            reservedBytes -= bytes;
            removeClosedEmptyAccountLocked(account);
            if (backpressured && reservedBytes <= lowWatermarkBytes) {
                backpressured = false;
            }
            granted = closed || backpressured ? null : grantOneWaiterLocked();
        }
        scheduleGranted(granted);
    }

    public void cancelWaiter(InboundConnectionMemory connection) {
        Objects.requireNonNull(connection, "connection");
        Granted granted;
        synchronized (lock) {
            removeWaiterLocked(connection);
            granted = closed || backpressured ? null : grantOneWaiterLocked();
        }
        scheduleGranted(granted);
    }

    public InboundMemoryBudgetStats stats() {
        synchronized (lock) {
            return new InboundMemoryBudgetStats(
                    capacityBytes,
                    reservedBytes,
                    waitersByConnection.size(),
                    backpressured,
                    rejectedConnections,
                    peakReservedBytes,
                    readCreditBytes,
                    retainedInputCapacityBytes,
                    consolidationBytes,
                    closed
            );
        }
    }

    int attachedAccountCountForTests() {
        synchronized (lock) {
            return accounts.size();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            backpressured = false;
            waiters.clear();
            waitersByConnection.clear();
        }
    }

    void closeConnection(InboundConnectionMemory connection) {
        Objects.requireNonNull(connection, "connection");
        Granted granted;
        synchronized (lock) {
            ConnectionMemoryAccount account = connection.account();
            long undeliveredReservation = connection.clearGrantedReservation();
            if (undeliveredReservation >= 0L) {
                if (!accounts.containsKey(account)) {
                    throw new IllegalStateException("connection memory account is not attached to this budget");
                }
                account.releaseReserved(undeliveredReservation);
                if (undeliveredReservation > reservedBytes) {
                    throw new IllegalStateException("inbound memory release exceeds reservation");
                }
                reservedBytes -= undeliveredReservation;
                if (backpressured && reservedBytes <= lowWatermarkBytes) {
                    backpressured = false;
                }
            }
            account.markClosed();
            removeWaiterLocked(connection);
            removeClosedEmptyAccountLocked(account);
            granted = closed || backpressured ? null : grantOneWaiterLocked();
        }
        scheduleGranted(granted);
    }

    void adjustReadCredit(long delta) {
        synchronized (lock) {
            readCreditBytes = adjustCounter(readCreditBytes, delta, "read credit");
        }
    }

    void adjustRetainedInputCapacity(long delta) {
        synchronized (lock) {
            retainedInputCapacityBytes = adjustCounter(retainedInputCapacityBytes, delta, "retained input capacity");
        }
    }

    void adjustConsolidation(long delta) {
        synchronized (lock) {
            consolidationBytes = adjustCounter(consolidationBytes, delta, "consolidation");
        }
    }

    private ReservationResult tryAdmit(InboundConnectionMemory connection, long bytes, long inputCapacityReleasedAfterCopy) {
        Objects.requireNonNull(connection, "connection");
        if (bytes < 0L || inputCapacityReleasedAfterCopy < 0L) {
            throw new IllegalArgumentException("reservation bytes must be non-negative");
        }

        synchronized (lock) {
            connection.attach(this);
            ConnectionMemoryAccount account = connection.account();
            if (closed || account.closed()) {
                return ReservationResult.CLOSED;
            }
            accounts.put(account, Boolean.TRUE);
            if (inputCapacityReleasedAfterCopy > account.reservedBytes()) {
                throw new IllegalArgumentException("input release credit exceeds connection reservation");
            }
            if (!fitsConnection(account, bytes, inputCapacityReleasedAfterCopy)
                    || bytes > capacityBytes) {
                rejectedConnections = saturatedAdd(rejectedConnections, 1L);
                return ReservationResult.REQUEST_LIMIT;
            }
            if (waitersByConnection.containsKey(connection)) {
                return ReservationResult.WAITING;
            }
            // 复制/解码替换会在成功后释放已持有的完整输入组件；阻塞该路径会使高水位下的半包无法收敛。
            if ((backpressured && inputCapacityReleasedAfterCopy == 0L) || !fitsGlobal(bytes)) {
                Waiter waiter = new Waiter(connection, bytes, inputCapacityReleasedAfterCopy);
                waiters.addLast(waiter);
                waitersByConnection.put(connection, waiter);
                return ReservationResult.WAITING;
            }
            reserveLocked(account, bytes);
            return ReservationResult.RESERVED;
        }
    }

    private Granted grantOneWaiterLocked() {
        while (!waiters.isEmpty()) {
            Waiter waiter = waiters.peekFirst();
            InboundConnectionMemory connection = waiter.connection;
            ConnectionMemoryAccount account = connection.account();
            if (account.closed()) {
                waiters.removeFirst();
                waitersByConnection.remove(connection);
                continue;
            }
            if (!fitsConnection(account, waiter.bytes, waiter.inputCapacityReleasedAfterCopy)
                    || !fitsGlobal(waiter.bytes)) {
                return null;
            }
            waiters.removeFirst();
            waitersByConnection.remove(connection);
            reserveLocked(account, waiter.bytes);
            return new Granted(connection, waiter.bytes);
        }
        return null;
    }

    private void scheduleGranted(Granted granted) {
        if (granted == null) {
            return;
        }
        granted.connection.markGrantedReservation(granted.bytes);
        if (granted.connection.scheduleResume()) {
            return;
        }
        granted.connection.close();
    }

    private void reserveLocked(ConnectionMemoryAccount account, long bytes) {
        account.addReserved(bytes);
        reservedBytes = saturatedAdd(reservedBytes, bytes);
        peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
        if (highWatermarkBytes > 0L && reservedBytes >= highWatermarkBytes) {
            backpressured = true;
        }
    }

    private boolean fitsConnection(ConnectionMemoryAccount account, long bytes, long releasedAfterCopy) {
        long retainedAfterCopy = account.reservedBytes() - releasedAfterCopy;
        return saturatedAdd(retainedAfterCopy, bytes) <= account.hardLimitBytes();
    }

    private boolean fitsGlobal(long bytes) {
        return saturatedAdd(reservedBytes, bytes) <= capacityBytes;
    }

    private void removeWaiterLocked(InboundConnectionMemory connection) {
        Waiter waiter = waitersByConnection.remove(connection);
        if (waiter != null) {
            waiters.remove(waiter);
        }
    }

    private void removeClosedEmptyAccountLocked(ConnectionMemoryAccount account) {
        if (account.closed() && account.reservedBytes() == 0L) {
            accounts.remove(account);
        }
    }

    private static long highWatermark(long capacityBytes) {
        if (capacityBytes == 0L) {
            return 0L;
        }
        long quotient = capacityBytes / 4L;
        long remainder = capacityBytes % 4L;
        return saturatedAdd(quotient * 3L, (remainder * 3L + 3L) / 4L);
    }

    private static long adjustCounter(long current, long delta, String name) {
        if (delta >= 0L) {
            return saturatedAdd(current, delta);
        }
        if (delta == Long.MIN_VALUE) {
            throw new IllegalStateException(name + " counter underflow");
        }
        long amount = -delta;
        if (amount > current) {
            throw new IllegalStateException(name + " counter underflow");
        }
        return current - amount;
    }

    static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private record Waiter(InboundConnectionMemory connection, long bytes, long inputCapacityReleasedAfterCopy) {
    }

    private record Granted(InboundConnectionMemory connection, long bytes) {
    }
}
