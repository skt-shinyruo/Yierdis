package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

/**
 * Enforces Redis-style "single thread DB semantics".
 * <p>
 * The DB must be explicitly bound to exactly one thread (typically the command executor thread).
 * Any access before binding or from a non-owner thread fails fast to surface misuse early.
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

