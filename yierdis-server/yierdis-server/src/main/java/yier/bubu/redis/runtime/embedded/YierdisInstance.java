package yier.bubu.redis.runtime.embedded;

// YierdisInstance：提供可嵌入（embedded）的 instance API（Netty-free），负责装配多 DB、路由与资源生命周期。

import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

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
    private final YierdisInstanceResources resources;
    private final YierdisInstanceRuntimeAccess runtimeAccess;
    private final YierdisInstanceObservability observability;

    private boolean closed;

    private YierdisInstance(
            YierdisInstanceConfig config,
            YierdisInstanceResources resources
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.runtimeAccess = new YierdisInstanceRuntimeAccess(this);
        this.observability = new YierdisInstanceObservability(this);
    }

    public static YierdisInstance create(YierdisInstanceConfig config) {
        Objects.requireNonNull(config, "config");
        YierdisDbEngineFactory engineFactory = new YierdisDbEngineFactory(
                YierdisFfmStableMemoryBackend::new,
                config.nativeSlotCapacity()
        );
        int databases = Math.max(1, config.databases());
        boolean perDbScope = config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB;
        long perDbMaxmemory = 0;
        long remainder = 0;
        if (perDbScope && config.maxmemoryBytes() > 0) {
            perDbMaxmemory = config.maxmemoryBytes() / (long) databases;
            remainder = config.maxmemoryBytes() - perDbMaxmemory * (long) databases;
            if (remainder < 0) {
                remainder = 0;
            }
        }

        YierdisDb[] dbs = new YierdisDb[databases];
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
                dbs[i] = engineFactory.create(new DbEngineConfig(
                        i,
                        dbMax,
                        config.maxmemoryPolicy(),
                        config.maxmemorySamples(),
                        config.evictionTimeLimitMillis(),
                        config.expireCleanupTimeLimitMillis(),
                        config.defrag()
                ));
            }

            YierdisGlobalMaxmemoryGovernor governor = null;
            if (!perDbScope && config.maxmemoryBytes() > 0) {
                governor = new YierdisGlobalMaxmemoryGovernor(
                        dbs,
                        config.maxmemoryBytes(),
                        config.maxmemoryPolicy(),
                        config.maxmemorySamples(),
                        TimeUnit.MILLISECONDS.toNanos(config.evictionTimeLimitMillis())
                );

                for (YierdisDb db : dbs) {
                    db.attachMaxmemoryCoordinator(governor);
                }
            }

            return new YierdisInstance(
                    config,
                    new YierdisInstanceResources(dbs, governor)
            );
        } catch (Throwable t) {
            throw YierdisInstanceResources.startupFailure(t, dbs);
        }
    }

    public YierdisInstanceConfig config() {
        return config;
    }

    public int databases() {
        return resources.databases();
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
     * 获取所有 DB 的能力视图数组（{@link DbEngine}）。
     * <p>
     * 返回的是一个防御性拷贝，避免暴露底层实现数组并规避协变数组写入风险。
     */
    public DbEngine[] engines() {
        return resources.engineViews();
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
        return resources.engine(dbIndex);
    }

    YierdisInstanceResources resources() {
        return resources;
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

}
