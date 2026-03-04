package yier.bubu.redis.runtime;

import yier.bubu.redis.ops.DbEngine;

import java.util.Objects;

/**
 * Instance-level background maintenance (Netty-free).
 * <p>
 * This component centralizes the "periodic maintenance tick" logic so that:
 * <ul>
 *   <li>Server bootstrap only needs to <b>schedule</b> a tick; the maintenance policy stays in runtime.</li>
 *   <li>Embedded/runtime users can reuse the same maintenance behavior.</li>
 * </ul>
 * <p>
 * Threading note: callers must ensure the underlying {@link YierdisInstance} is bound to the current owner thread
 * before invoking {@link #maintenanceTick()} (see {@link YierdisInstance#bindToCurrentThread()}).
 */
public final class YierdisInstanceMaintenance {
    private final YierdisInstance instance;

    public YierdisInstanceMaintenance(YierdisInstance instance) {
        this.instance = Objects.requireNonNull(instance, "instance");
    }

    /**
     * Perform a single best-effort maintenance tick:
     * <ul>
     *   <li>cleanup expired keys for every DB</li>
     *   <li>enforce maxmemory (per-db or global) when configured</li>
     * </ul>
     */
    public void maintenanceTick() {
        YierdisInstance inst = instance;
        YierdisInstanceConfig cfg = inst.config();
        int databases = Math.max(0, inst.databases());
        if (databases == 0) {
            return;
        }

        boolean maxmemoryEnabled = cfg.maxmemoryBytes() > 0;
        boolean perDb = maxmemoryEnabled && cfg.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB;
        boolean global = maxmemoryEnabled && cfg.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;

        DbEngine firstEngine = null;
        for (int i = 0; i < databases; i++) {
            DbEngine engine = inst.engine(i);
            if (engine == null) {
                continue;
            }
            if (firstEngine == null) {
                firstEngine = engine;
            }

            engine.expiration().cleanupExpired();
            if (perDb) {
                engine.eviction().enforceMaxmemory();
            }
        }

        if (global && firstEngine != null) {
            firstEngine.eviction().enforceMaxmemory();
        }
    }
}
