package yier.bubu.redis.runtime.embedded;

// YierdisInstance：提供可嵌入（embedded）的 instance API（Netty-free），负责装配多 DB、路由与资源生命周期。

import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.util.ArrayList;
import java.util.List;
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
        YierdisInstanceConfig.EngineFactoryBinding binding = config.engineFactoryBinding();
        DbEngineFactory engineFactory = binding == null ? config.engineFactory() : binding.engineFactory();
        if (engineFactory == null) {
            throw new IllegalArgumentException("engineFactory must be configured");
        }
        AutoCloseable ownedResource = binding == null ? null : binding.ownedResource();
        return create(config, engineFactory, ownedResource);
    }

    private static YierdisInstance create(
            YierdisInstanceConfig config,
            DbEngineFactory engineFactory,
            AutoCloseable ownedEngineFactoryResource
    ) {
        int databases = Math.max(1, config.databases());
        boolean perDbScope = config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB;
        CommitStream commitStream = config.changeSink() == YierdisChangeSink.NOOP
                ? null
                : CommitStream.prepare(
                        config.changeSink(),
                        config.commitStreamMaxEvents(),
                        config.commitStreamMaxRetainedBytes(),
                        config.commitStreamShutdownTimeoutMillis()
                );
        DbCommitPublisher commitPublisher = commitStream == null ? DbCommitPublisher.NOOP : commitStream;

        long perDbMaxmemory = 0;
        long remainder = 0;
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

            YierdisGlobalMaxmemoryGovernor governor = null;
            if (!perDbScope && config.maxmemoryBytes() > 0) {
                RuntimeDbEngine[] participants = new RuntimeDbEngine[dbs.length];
                for (int i = 0; i < dbs.length; i++) {
                    participants[i] = dbs[i];
                }

                governor = new YierdisGlobalMaxmemoryGovernor(
                        participants,
                        config.maxmemoryBytes(),
                        config.maxmemoryPolicy(),
                        config.maxmemorySamples(),
                        TimeUnit.MILLISECONDS.toNanos(config.evictionTimeLimitMillis())
                );

                for (RuntimeDbEngine engine : dbs) {
                    if (engine == null) {
                        continue;
                    }
                    engine.attachMaxmemoryCoordinator(governor);
                }
            }

            for (int index = 0; index < dbs.length; index++) {
                RuntimeDbEngine engine = dbs[index];
                if (engine != null) {
                    engine.attachCommitPublisher(commitPublisher, index);
                }
            }
            if (commitStream != null) {
                commitStream.start();
            }

            return new YierdisInstance(
                    config,
                    new YierdisInstanceResources(dbs, closeables(ownedEngineFactoryResource), governor, commitStream)
            );
        } catch (Throwable t) {
            if (commitStream != null) {
                try {
                    commitStream.close();
                } catch (Throwable closeFailure) {
                    t.addSuppressed(closeFailure);
                }
            }
            throw YierdisInstanceResources.startupFailure(t, dbs, closeables(ownedEngineFactoryResource));
        }
    }

    private static List<AutoCloseable> closeables(AutoCloseable resource) {
        if (resource == null) {
            return List.of();
        }
        List<AutoCloseable> closeables = new ArrayList<>(1);
        closeables.add(resource);
        return closeables;
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
     * 获取 DB 的能力视图（依赖倒置到 {@link DbEngine}），避免上层（例如 server/bootstrap）直接依赖具体实现类。
     */
    public DbEngine engine(int dbIndex) {
        return resources.engine(dbIndex);
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
        return resources.engine(dbIndex);
    }

    CommitStream commitStream() {
        return resources.commitStream();
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
