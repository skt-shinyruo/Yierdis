package yier.bubu.redis.runtime;

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
        instance.resources().bindToCurrentThread();
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

        for (int i = 0; i < databases; i++) {
            RuntimeDbEngine engine = engine(i);
            if (engine == null) {
                continue;
            }
            engine.expiration().cleanupExpired();
            if (perDb) {
                engine.enforceMaxmemoryMaintenance();
            }
        }

        if (global) {
            instance.resources().enforceGlobalMaxmemoryMaintenance();
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

        instance.resources().shutdownAll();
    }

    RuntimeDbEngine engine(int dbIndex) {
        return instance.runtimeEngine(dbIndex);
    }
}
