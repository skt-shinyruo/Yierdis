package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

/**
 * 维护 Redis 风格的单线程 DB 访问语义。
 *
 * <p>DB 必须显式绑定到一个 owner thread（通常是命令执行线程）。绑定前访问、跨线程访问或关闭后访问都会 fail-fast，
 * 让调用方尽早暴露生命周期误用。</p>
 */
public final class DbThreadGuard {
    private volatile Thread ownerThread;
    private volatile boolean closed;

    void bindToCurrentThread() {
        if (closed) {
            throw new IllegalStateException("YierdisDb is closed");
        }
        Thread current = Thread.currentThread();
        Thread owner = ownerThread;
        if (owner == null) {
            ownerThread = current;
            return;
        }
        if (owner != current) {
            throw new IllegalStateException("YierdisDb already bound to a different thread");
        }
    }

    void checkThread() {
        if (closed) {
            throw new IllegalStateException("YierdisDb is closed");
        }
        Thread owner = ownerThread;
        if (owner == null) {
            throw new IllegalStateException("YierdisDb accessed before bindToCurrentThread()");
        }
        if (owner != Thread.currentThread()) {
            throw new IllegalStateException("YierdisDb accessed from non-owner thread");
        }
    }

    void checkThreadForShutdown() {
        Thread owner = ownerThread;
        if (owner != null && owner != Thread.currentThread()) {
            throw new IllegalStateException("YierdisDb shutdown from non-owner thread");
        }
    }

    boolean tryMarkClosed() {
        if (closed) {
            return false;
        }
        closed = true;
        return true;
    }
}
