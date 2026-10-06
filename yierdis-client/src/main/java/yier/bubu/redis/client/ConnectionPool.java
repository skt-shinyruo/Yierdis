package yier.bubu.redis.client;

import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 持有多条指向同一台 standalone Yierdis 的连接，一次把一条借给调用方。
 * <p>
 * 最大连接数和借出等待由调用方传入。等待 {@code 0} 只试一次：没有可借的空闲连接，
 * 并且每条名额都已借出时，立刻以 {@link BorrowException} 失败，不等别的线程归还。
 * 名额还没用完时，这次借出仍会打开一条连接。等待者按到达顺序获得连接。
 * <p>
 * 借出不发送 {@code PING}，也不为核对 DB 再发 {@code SELECT}。
 * 新建连接走 {@link Connection#connect(ConnectionSettings)}；DB 不是 0 时由那里先 {@code SELECT}，失败则不借出。
 * 借出等待耗尽是 {@link BorrowException}，不关闭已经借出的连接。
 * 打开新连接时，连接超时和剩余借出等待谁先到，这次借出就失败，没交出去的套接字会被放弃。
 * <p>
 * {@link #returnConnection(Connection)} 不代发 {@code sync()}、{@code DISCARD} 或 {@code EXEC}。
 * 不在普通模式，或 DB 下标不是池的下标时，关掉这条连接并不放回。
 * 服务端错误之后仍在普通模式、且还在池的 DB 上的连接可以放回并再次借出。
 * {@link #close()} 之后不能再借；空闲连接立刻关掉；已经借出的连接可以做完当前命令，归还时关掉。
 */
public final class ConnectionPool implements AutoCloseable {
    private final ConnectionSettings settings;
    private final int maximumSize;
    private final long borrowWaitMillis;
    private final Object lock = new Object();
    private final ArrayDeque<Connection> idle = new ArrayDeque<>();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final Set<Connection> borrowed = Collections.newSetFromMap(new IdentityHashMap<>());
    private int connecting;
    private boolean closed;

    public ConnectionPool(ConnectionSettings settings, int maximumSize, long borrowWaitMillis) {
        this.settings = Objects.requireNonNull(settings, "settings");
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be > 0");
        }
        if (borrowWaitMillis < 0) {
            throw new IllegalArgumentException("borrowWaitMillis must be >= 0");
        }
        this.maximumSize = maximumSize;
        this.borrowWaitMillis = borrowWaitMillis;
    }

    public Connection borrow() {
        Waiter self = new Waiter();
        boolean queued = false;
        long deadlineNanos = borrowWaitMillis > 0 ? deadlineNanos(borrowWaitMillis) : 0L;
        synchronized (lock) {
            while (true) {
                if (self.connection != null) {
                    queued = false;
                    return self.connection;
                }
                if (closed) {
                    leaveQueue(self, queued);
                    throw new BorrowException("pool is closed");
                }
                // 队列非空时只有队首可以取空闲连接或新建。后来的调用方继续排在后面。
                if (isFirst(self)) {
                    Connection ready = pollUsableIdle();
                    if (ready != null) {
                        leaveQueue(self, queued);
                        borrowed.add(ready);
                        return ready;
                    }
                    if ((long) borrowed.size() + connecting < maximumSize) {
                        if (borrowWaitMillis > 0 && remainingMillis(deadlineNanos) <= 0) {
                            leaveQueue(self, queued);
                            throw new BorrowException("borrow wait elapsed");
                        }
                        leaveQueue(self, queued);
                        connecting++;
                        break;
                    }
                }
                if (borrowWaitMillis == 0) {
                    // 等待 0 不挂起。没有可借的空闲连接、名额也已经满时立刻失败，不等别的线程归还。
                    leaveQueue(self, queued);
                    throw new BorrowException("no connection available");
                }
                if (!queued) {
                    waiters.addLast(self);
                    queued = true;
                }
                long waitMillis = remainingMillis(deadlineNanos);
                // wait(0) 会一直等。剩余时间不足 1 毫秒时直接失败，而不是当成无限等待。
                if (waitMillis <= 0) {
                    leaveQueue(self, queued);
                    throw new BorrowException("borrow wait elapsed");
                }
                try {
                    lock.wait(waitMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    if (self.connection != null) {
                        return self.connection;
                    }
                    leaveQueue(self, queued);
                    throw new BorrowException("borrow interrupted", interrupted);
                }
            }
        }
        return openForBorrow(deadlineNanos);
    }

    public void returnConnection(Connection connection) {
        Objects.requireNonNull(connection, "connection");
        boolean closeOnReturn;
        synchronized (lock) {
            if (!borrowed.remove(connection)) {
                throw new IllegalStateException("connection is not borrowed from this pool");
            }
            closeOnReturn = closed
                    || connection.isClosed()
                    || !connection.inNormalMode()
                    || connection.database() != settings.database();
            if (!closeOnReturn) {
                Waiter head = waiters.pollFirst();
                if (head != null) {
                    // 直接交给队首。连接不进空闲队列，后来的等待者抢不到这次归还。
                    head.connection = connection;
                    borrowed.add(connection);
                } else {
                    idle.addLast(connection);
                }
            }
            lock.notifyAll();
        }
        if (closeOnReturn && !connection.isClosed()) {
            // 不放回时只 close()。不发 sync、DISCARD 或 EXEC，未读回复和未提交的事务不会被池代为处理。
            connection.close();
        }
    }

    @Override
    public void close() {
        List<Connection> idleConnections;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            idleConnections = new ArrayList<>(idle);
            idle.clear();
            lock.notifyAll();
        }
        for (Connection connection : idleConnections) {
            connection.close();
        }
    }

    private Connection openForBorrow(long deadlineNanos) {
        Connection created = null;
        RuntimeException failure = null;
        Error error = null;
        try {
            created = openWithin(deadlineNanos);
        } catch (RuntimeException e) {
            failure = e;
        } catch (Error e) {
            error = e;
        }
        boolean handOut = false;
        synchronized (lock) {
            connecting--;
            if (failure == null && error == null && !closed) {
                borrowed.add(created);
                handOut = true;
            }
            lock.notifyAll();
        }
        if (!handOut && created != null) {
            created.close();
        }
        if (error != null) {
            throw error;
        }
        if (failure != null) {
            throw failure;
        }
        if (!handOut) {
            throw new BorrowException("pool is closed");
        }
        return created;
    }

    private Connection openWithin(long deadlineNanos) {
        long socketTimeoutMillis = settings.connectTimeoutMillis();
        long setupTimeoutMillis = settings.commandTimeoutMillis();
        if (borrowWaitMillis > 0) {
            long remaining = remainingMillis(deadlineNanos);
            if (remaining <= 0) {
                // 剩余借出等待已经用尽，不再打开套接字。
                throw new BorrowException("borrow wait elapsed");
            }
            // 连接超时和剩余借出等待谁先到，这次打开就失败。没交给调用方的连接由 connect 或下面的失败路径关掉。
            // 这种超时是借出失败，不是命令读超时。
            socketTimeoutMillis = Math.min(socketTimeoutMillis, remaining);
            setupTimeoutMillis = Math.min(setupTimeoutMillis, remaining);
        }
        try {
            return Connection.connect(settings, socketTimeoutMillis, setupTimeoutMillis);
        } catch (CommandTimeoutException e) {
            throw new BorrowException("timed out opening a connection", e);
        } catch (ConnectionException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                throw new BorrowException("timed out opening a connection", e);
            }
            throw e;
        }
    }

    private void leaveQueue(Waiter self, boolean queued) {
        if (queued) {
            waiters.remove(self);
        }
    }

    private boolean isFirst(Waiter self) {
        Waiter head = waiters.peekFirst();
        return head == null || head == self;
    }

    private Connection pollUsableIdle() {
        Connection connection;
        while ((connection = idle.pollFirst()) != null) {
            if (!connection.isClosed()) {
                return connection;
            }
        }
        return null;
    }

    private static long deadlineNanos(long borrowWaitMillis) {
        long now = System.nanoTime();
        long waitNanos;
        try {
            waitNanos = TimeUnit.MILLISECONDS.toNanos(borrowWaitMillis);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
        long deadline = now + waitNanos;
        if (waitNanos > 0 && deadline < now) {
            return Long.MAX_VALUE;
        }
        return deadline;
    }

    private static long remainingMillis(long deadlineNanos) {
        long nanos = deadlineNanos - System.nanoTime();
        if (nanos < 1_000_000L) {
            return 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static final class Waiter {
        private Connection connection;
    }
}
