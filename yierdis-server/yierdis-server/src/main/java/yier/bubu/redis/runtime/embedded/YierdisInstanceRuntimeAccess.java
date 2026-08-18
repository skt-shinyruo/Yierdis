package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.util.Objects;

/**
 * Owner-thread-only runtime seam for lifecycle and maintenance operations.
 * <p>
 * This keeps the public {@link yier.bubu.redis.storage.api.DbEngine} view stable for command/runtime consumers while still
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

        runMaintenanceTick(config, databases);
    }

    /**
     * 仅推进命令已发布的延迟资源回收，不执行过期清理、rehash、defrag 或 maxmemory 维护。
     */
    public void deferredReclamationTick() {
        instance.requireOpenRuntimeAccess();
        int databases = Math.max(0, instance.databases());
        for (int i = 0; i < databases; i++) {
            engine(i).runDeferredReclamation();
        }
    }

    private void runMaintenanceTick(YierdisInstanceConfig config, int databases) {
        boolean defragEnabled = config.defrag().enabled();

        for (int i = 0; i < databases; i++) {
            RuntimeDbEngine engine = engine(i);
            engine.runMaintenance();
            if (defragEnabled) {
                engine.defragMaintenance();
            }
        }

        instance.resources().enforceGlobalMaxmemoryMaintenance();
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
