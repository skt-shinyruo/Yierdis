package yier.bubu.redis.runtime.embedded;

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
    private final YierdisInstanceRuntimeAccess runtimeAccess;

    public YierdisInstanceMaintenance(YierdisInstance instance) {
        this(Objects.requireNonNull(instance, "instance").runtimeAccess());
    }

    YierdisInstanceMaintenance(YierdisInstanceRuntimeAccess runtimeAccess) {
        this.runtimeAccess = Objects.requireNonNull(runtimeAccess, "runtimeAccess");
    }

    /**
     * Perform a single best-effort maintenance tick:
     * <ul>
     *   <li>cleanup expired keys for every DB</li>
     *   <li>enforce maxmemory (per-db or global) when configured</li>
     * </ul>
     */
    public void maintenanceTick() {
        runtimeAccess.maintenanceTick();
    }
}
