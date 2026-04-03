package yier.bubu.redis.runtime;

import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.ops.RuntimeDbEngine;

import java.util.Objects;

/**
 * Owner-thread-only runtime seam for lifecycle and maintenance operations.
 * <p>
 * This keeps the public {@link yier.bubu.redis.ops.DbEngine} view stable for command/runtime consumers while still
 * allowing runtime/server lifecycle code to operate on {@link RuntimeDbEngine} directly when thread ownership matters.
 */
public final class YierdisInstanceRuntimeAccess implements AutoCloseable {
    private final YierdisInstance instance;

    YierdisInstanceRuntimeAccess(YierdisInstance instance) {
        this.instance = Objects.requireNonNull(instance, "instance");
    }

    /**
     * Bind all runtime DB engines to the current owner thread.
     */
    public void bindToCurrentThread() {
        instance.requireOpenRuntimeAccess();
        for (int i = 0; i < instance.databases(); i++) {
            RuntimeDbEngine engine = engine(i);
            if (engine == null) {
                continue;
            }
            engine.bindToCurrentThread();
        }
    }

    /**
     * Run one maintenance tick on the current owner thread.
     */
    public void maintenanceTick() {
        instance.requireOpenRuntimeAccess();
        YierdisInstanceConfig config = instance.config();
        int databases = Math.max(0, instance.databases());
        if (databases == 0) {
            return;
        }

        boolean maxmemoryEnabled = config.maxmemoryBytes() > 0;
        boolean perDb = maxmemoryEnabled && config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB;
        boolean global = maxmemoryEnabled && config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;

        RuntimeDbEngine firstEngine = null;
        for (int i = 0; i < databases; i++) {
            RuntimeDbEngine engine = engine(i);
            if (engine == null) {
                continue;
            }
            if (firstEngine == null) {
                firstEngine = engine;
            }

            engine.expiration().cleanupExpired();
            if (perDb) {
                engine.enforceMaxmemoryMaintenance();
            }
        }

        if (global && firstEngine != null) {
            firstEngine.enforceMaxmemoryMaintenance();
        }
    }

    /**
     * Best-effort runtime shutdown. If DBs were previously bound, callers must invoke this from the owner thread.
     */
    @Override
    public void close() {
        if (!instance.markClosed()) {
            return;
        }

        Throwable failure = null;
        for (int i = 0; i < instance.databases(); i++) {
            RuntimeDbEngine engine = engine(i);
            if (engine == null) {
                continue;
            }
            try {
                engine.shutdown();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }

        YierdisFfmMemoryRuntime memoryRuntime = instance.runtimeMemoryRuntime();
        if (instance.runtimeClosesMemoryRuntime() && memoryRuntime != null) {
            try {
                memoryRuntime.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }

        rethrowIfNeeded(failure);
    }

    RuntimeDbEngine engine(int dbIndex) {
        return instance.runtimeEngine(dbIndex);
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
