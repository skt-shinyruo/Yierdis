package yier.bubu.redis.runtime;

// YierdisInstance：提供可嵌入（embedded）的 instance API（Netty-free），负责装配多 DB、路由与资源生命周期。

import yier.bubu.redis.db.YierdisDbEngineFactory;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.MaxmemoryCoordinatorAware;
import yier.bubu.redis.ops.MaxmemoryParticipant;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.ops.MaxmemoryUsageSource;
import yier.bubu.redis.ops.RuntimeDbEngine;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 可嵌入（embedded）的 instance API（Netty-free）。
 * <p>
 * 线程语义（SSOT）：所有 DB 访问必须在 {@link #bindToCurrentThread()} 绑定后的 owner thread 上进行；
 * 未绑定或跨线程访问会 fail-fast，以避免静默竞态与一致性风险。
 */
public final class YierdisInstance implements AutoCloseable {
    private final YierdisInstanceConfig config;
    private final RuntimeDbEngine[] dbs;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final boolean closeMemoryRuntime;
    private final YierdisInstanceRuntimeAccess runtimeAccess;
    private final YierdisInstanceObservability observability;

    private boolean closed;

    private YierdisInstance(
            YierdisInstanceConfig config,
            RuntimeDbEngine[] dbs,
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean closeMemoryRuntime
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.dbs = Objects.requireNonNull(dbs, "dbs");
        this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
        this.closeMemoryRuntime = closeMemoryRuntime;
        this.runtimeAccess = new YierdisInstanceRuntimeAccess(this);
        this.observability = new YierdisInstanceObservability(this);
    }

    public static YierdisInstance create(YierdisInstanceConfig config) {
        Objects.requireNonNull(config, "config");
        int databases = Math.max(1, config.databases());

        YierdisFfmMemoryRuntime memoryRuntime = new YierdisFfmMemoryRuntime("instance");
        DbEngineFactory engineFactory = config.engineFactory();
        if (engineFactory == null) {
            engineFactory = new YierdisDbEngineFactory(memoryRuntime);
        }

        long perDbMaxmemory = 0;
        long remainder = 0;
        boolean perDbScope = config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB;
        if (perDbScope && config.maxmemoryBytes() > 0) {
            perDbMaxmemory = config.maxmemoryBytes() / (long) databases;
            remainder = config.maxmemoryBytes() - perDbMaxmemory * (long) databases;
            if (remainder < 0) {
                remainder = 0;
            }
        }

        RuntimeDbEngine[] dbs = new RuntimeDbEngine[databases];
        try {
            for (int i = 0; i < databases; i++) {
                long dbMax = config.maxmemoryBytes();
                if (perDbScope) {
                    dbMax = perDbMaxmemory;
                    if (remainder > 0) {
                        dbMax++;
                        remainder--;
                    }
                }
                dbs[i] = engineFactory.create(
                        i,
                        dbMax,
                        config.maxmemoryPolicy(),
                        config.maxmemorySamples(),
                        config.evictionTimeLimitMillis(),
                        config.expireCleanupTimeLimitMillis()
                );
            }

            if (!perDbScope && config.maxmemoryBytes() > 0) {
                MaxmemoryParticipant[] participants = new MaxmemoryParticipant[dbs.length];
                for (int i = 0; i < dbs.length; i++) {
                    RuntimeDbEngine engine = dbs[i];
                    if (engine == null) {
                        continue;
                    }
                    if (!(engine instanceof MaxmemoryParticipant participant)) {
                        throw new IllegalStateException("GLOBAL maxmemory requires MaxmemoryParticipant: dbIndex=" + i);
                    }
                    participants[i] = participant;
                }

                MaxmemoryUsageSource[] sharedUsage = new MaxmemoryUsageSource[]{
                        () -> {
                            try {
                                return Math.max(0L, memoryRuntime.usedBytes());
                            } catch (Throwable ignored) {
                                return 0L;
                            }
                        }
                };

                YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                        participants,
                        sharedUsage,
                        config.maxmemoryBytes(),
                        MaxmemoryPolicy.parse(config.maxmemoryPolicy()),
                        config.maxmemorySamples(),
                        TimeUnit.MILLISECONDS.toNanos(config.evictionTimeLimitMillis())
                );

                for (RuntimeDbEngine engine : dbs) {
                    if (engine == null) {
                        continue;
                    }
                    if (!(engine instanceof MaxmemoryCoordinatorAware aware)) {
                        throw new IllegalStateException("GLOBAL maxmemory requires MaxmemoryCoordinatorAware");
                    }
                    aware.attachMaxmemoryCoordinator(governor);
                }
            }

            return new YierdisInstance(config, dbs, memoryRuntime, true);
        } catch (Throwable t) {
            throw startupFailure(t, dbs, memoryRuntime);
        }
    }

    public YierdisInstanceConfig config() {
        return config;
    }

    public int databases() {
        return dbs.length;
    }

    /**
     * Owner-thread-only runtime seam for maintenance and shutdown orchestration.
     */
    public YierdisInstanceRuntimeAccess runtimeAccess() {
        return runtimeAccess;
    }

    /**
     * Runtime-owned observability seam for instance-wide summaries.
     */
    public YierdisInstanceObservability observability() {
        return observability;
    }

    /**
     * 获取 DB 的能力视图（依赖倒置到 {@link DbEngine}），避免上层（例如 server/bootstrap）直接依赖具体实现类。
     */
    public DbEngine engine(int dbIndex) {
        return dbInternal(dbIndex);
    }

    /**
     * 获取所有 DB 的能力视图数组（{@link DbEngine}）。
     * <p>
     * 返回的是一个防御性拷贝，避免暴露底层实现数组并规避协变数组写入风险。
     */
    public DbEngine[] engines() {
        DbEngine[] out = new DbEngine[dbs.length];
        for (int i = 0; i < dbs.length; i++) {
            out[i] = dbs[i];
        }
        return out;
    }

    private RuntimeDbEngine dbInternal(int dbIndex) {
        int idx = Math.max(0, dbIndex);
        if (idx >= dbs.length) {
            throw new IllegalArgumentException("dbIndex out of range: " + dbIndex);
        }
        return dbs[idx];
    }

    /**
     * 将该 instance 的所有 DB 绑定到当前线程（owner thread）。
     * <p>
     * 该操作通常由 server executor 在线程启动阶段执行；embedded 场景需在执行命令前显式调用。
     */
    public void bindToCurrentThread() {
        runtimeAccess.bindToCurrentThread();
    }

    /**
     * 关闭 instance：best-effort 释放 DB 与可选的 off-heap allocator。
     * <p>
     * 若 DB 已绑定，则需要在同一 owner thread 调用（或由 server executor 调度到 owner thread 执行）。
     */
    @Override
    public void close() {
        runtimeAccess.close();
    }

    RuntimeDbEngine runtimeEngine(int dbIndex) {
        return dbInternal(dbIndex);
    }

    YierdisFfmMemoryRuntime runtimeMemoryRuntime() {
        return memoryRuntime;
    }

    boolean runtimeClosesMemoryRuntime() {
        return closeMemoryRuntime;
    }

    void requireOpenRuntimeAccess() {
        if (closed) {
            throw new IllegalStateException("YierdisInstance is closed");
        }
    }

    boolean markClosed() {
        if (closed) {
            return false;
        }
        closed = true;
        return true;
    }

    private static RuntimeException startupFailure(
            Throwable failure,
            RuntimeDbEngine[] dbs,
            YierdisFfmMemoryRuntime memoryRuntime
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
                    cleanupFailure = recordSuppressedFailure(cleanupFailure, t);
                }
            }
        }
        if (memoryRuntime != null) {
            try {
                memoryRuntime.close();
            } catch (Throwable t) {
                cleanupFailure = recordSuppressedFailure(cleanupFailure, t);
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

    private static Throwable recordSuppressedFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }
}
