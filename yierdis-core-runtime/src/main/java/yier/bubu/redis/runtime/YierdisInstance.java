package yier.bubu.redis.runtime;

// YierdisInstance：提供可嵌入（embedded）的 instance API（Netty-free），负责装配多 DB、路由与资源生命周期。

import yier.bubu.redis.command.ServerInfoProvider;
import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.protocol.DbIndexProvider;

import java.util.Objects;

/**
 * 可嵌入（embedded）的 instance API（Netty-free）。
 * <p>
 * 线程语义（SSOT）：所有 DB 访问必须在 {@link #bindToCurrentThread()} 绑定后的 owner thread 上进行；
 * 未绑定或跨线程访问会 fail-fast，以避免静默竞态与一致性风险。
 */
public final class YierdisInstance implements AutoCloseable {
    private final YierdisInstanceConfig config;
    private final YierdisDb[] dbs;
    private final YierdisDbRouter router;
    private final YierdisOffHeapAllocator offHeapAllocator;
    private final boolean closeAllocator;

    private boolean closed;

    private YierdisInstance(
            YierdisInstanceConfig config,
            YierdisDb[] dbs,
            YierdisDbRouter router,
            YierdisOffHeapAllocator offHeapAllocator,
            boolean closeAllocator
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.dbs = Objects.requireNonNull(dbs, "dbs");
        this.router = Objects.requireNonNull(router, "router");
        this.offHeapAllocator = offHeapAllocator;
        this.closeAllocator = closeAllocator;
    }

    public static YierdisInstance create(YierdisInstanceConfig config) {
        Objects.requireNonNull(config, "config");
        int databases = Math.max(1, config.databases());

        YierdisOffHeapAllocator allocator = config.offHeapAllocator();

        // 约定：多 DB 场景默认共享 allocator，并由 instance 统一负责 close（避免 double-close 与 usedBytes 双计数）。
        boolean dbOwnsAllocator = config.ownsOffHeapAllocator() && databases == 1;
        boolean instanceClosesAllocator = config.ownsOffHeapAllocator() && !dbOwnsAllocator;

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

        YierdisDb[] dbs = new YierdisDb[databases];
        for (int i = 0; i < databases; i++) {
            long dbMax = config.maxmemoryBytes();
            if (perDbScope) {
                dbMax = perDbMaxmemory;
                if (remainder > 0) {
                    dbMax++;
                    remainder--;
                }
            }
            dbs[i] = new YierdisDb(
                    allocator,
                    dbOwnsAllocator,
                    config.offHeapKeysEnabled(),
                    dbMax,
                    config.maxmemoryPolicy(),
                    config.maxmemorySamples(),
                    config.evictionTimeLimitMillis(),
                    config.expireCleanupTimeLimitMillis()
            );
        }

        if (!perDbScope && config.maxmemoryBytes() > 0) {
            YierdisDb.enableGlobalMaxmemory(
                    dbs,
                    allocator,
                    config.maxmemoryBytes(),
                    config.maxmemoryPolicy(),
                    config.maxmemorySamples(),
                    config.evictionTimeLimitMillis()
            );
        }

        YierdisDbRouter router = new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexProvider dbIndexProvider) {
                if (dbs.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = 0;
                if (dbIndexProvider != null) {
                    idx = dbIndexProvider.dbIndex();
                }
                if (idx < 0 || idx >= dbs.length) {
                    idx = 0;
                }
                return dbs[idx];
            }

            @Override
            public int databases() {
                return Math.max(1, dbs.length);
            }
        };

        return new YierdisInstance(config, dbs, router, allocator, instanceClosesAllocator);
    }

    public YierdisInstanceConfig config() {
        return config;
    }

    public int databases() {
        return dbs.length;
    }

    /**
     * 获取 DB 的能力视图（依赖倒置到 {@link DbEngine}），避免上层（例如 server/bootstrap）直接依赖具体实现类。
     */
    public DbEngine engine(int dbIndex) {
        return db(dbIndex);
    }

    /**
     * 获取所有 DB 的能力视图数组（{@link DbEngine}）。
     * <p>
     * 返回的是底层数组的协变视图（实现类数组），调用方不应向该数组写入非 DB 实现对象。
     */
    public DbEngine[] engines() {
        return dbs;
    }

    public YierdisDb[] dbs() {
        return dbs;
    }

    public YierdisDb db(int dbIndex) {
        int idx = Math.max(0, dbIndex);
        if (idx >= dbs.length) {
            throw new IllegalArgumentException("dbIndex out of range: " + dbIndex);
        }
        return dbs[idx];
    }

    public YierdisDbRouter router() {
        return router;
    }

    public YierdisFastCommandProcessor newCommandProcessor() {
        return newCommandProcessor((ServerInfoProvider) null);
    }

    public YierdisFastCommandProcessor newCommandProcessor(ServerInfoProvider infoProvider) {
        return new YierdisFastCommandProcessor(router, infoProvider);
    }

    public YierdisFastCommandProcessor newCommandProcessor(SlowCommandGovernor slowGovernor) {
        return newCommandProcessor((ServerInfoProvider) null, slowGovernor);
    }

    public YierdisFastCommandProcessor newCommandProcessor(ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor) {
        return new YierdisFastCommandProcessor(router, infoProvider, slowGovernor);
    }

    /**
     * 将该 instance 的所有 DB 绑定到当前线程（owner thread）。
     * <p>
     * 该操作通常由 server executor 在线程启动阶段执行；embedded 场景需在执行命令前显式调用。
     */
    public void bindToCurrentThread() {
        if (closed) {
            throw new IllegalStateException("YierdisInstance is closed");
        }
        for (YierdisDb db : dbs) {
            if (db == null) {
                continue;
            }
            db.bindToCurrentThread();
        }
    }

    /**
     * 关闭 instance：best-effort 释放 DB 与可选的 off-heap allocator。
     * <p>
     * 若 DB 已绑定，则需要在同一 owner thread 调用（或由 server executor 调度到 owner thread 执行）。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        for (YierdisDb db : dbs) {
            if (db == null) {
                continue;
            }
            try {
                db.shutdown();
            } catch (Throwable ignored) {
                // best-effort close
            }
        }

        if (closeAllocator && offHeapAllocator != null) {
            try {
                offHeapAllocator.close();
            } catch (Throwable ignored) {
                // best-effort close
            }
        }
    }
}
