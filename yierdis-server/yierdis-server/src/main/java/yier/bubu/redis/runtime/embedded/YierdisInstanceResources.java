package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

import java.util.Objects;

/**
 * Runtime-owned resource graph for a Yierdis instance.
 */
final class YierdisInstanceResources {
    private final RuntimeDbEngine[] dbs;
    private final YierdisGlobalMaxmemoryGovernor globalMaxmemoryGovernor;

    YierdisInstanceResources(
            RuntimeDbEngine[] dbs,
            YierdisGlobalMaxmemoryGovernor globalMaxmemoryGovernor
    ) {
        Objects.requireNonNull(dbs, "dbs");
        this.dbs = dbs.clone();
        this.globalMaxmemoryGovernor = globalMaxmemoryGovernor;
    }

    int databases() {
        return dbs.length;
    }

    RuntimeDbEngine engine(int dbIndex) {
        int idx = Math.max(0, dbIndex);
        if (idx >= dbs.length) {
            throw new IllegalArgumentException("dbIndex out of range: " + dbIndex);
        }
        return dbs[idx];
    }

    DbEngine[] engineViews() {
        DbEngine[] out = new DbEngine[dbs.length];
        for (int i = 0; i < dbs.length; i++) {
            out[i] = dbs[i];
        }
        return out;
    }

    void enforceGlobalMaxmemoryMaintenance() {
        if (globalMaxmemoryGovernor != null) {
            globalMaxmemoryGovernor.enforceMaintenance();
        }
    }

    void bindToCurrentThread() {
        for (RuntimeDbEngine engine : dbs) {
            if (engine == null) {
                continue;
            }
            engine.bindToCurrentThread();
        }
    }

    void shutdownAll() {
        Throwable failure = null;
        for (RuntimeDbEngine engine : dbs) {
            if (engine == null) {
                continue;
            }
            try {
                engine.shutdown();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }

        rethrowIfNeeded(failure);
    }

    static RuntimeException startupFailure(
            Throwable failure,
            RuntimeDbEngine[] dbs
    ) {
        Throwable cleanupFailure = null;
        if (dbs != null) {
            for (RuntimeDbEngine engine : dbs) {
                if (engine == null) {
                    continue;
                }
                try {
                    engine.shutdown();
                } catch (Throwable t) {
                    cleanupFailure = recordFailure(cleanupFailure, t);
                }
            }
        }
        if (cleanupFailure != null) {
            failure.addSuppressed(cleanupFailure);
        }
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IllegalStateException(failure);
    }

    private static Throwable recordFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private static void rethrowIfNeeded(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("close failed", failure);
    }
}
