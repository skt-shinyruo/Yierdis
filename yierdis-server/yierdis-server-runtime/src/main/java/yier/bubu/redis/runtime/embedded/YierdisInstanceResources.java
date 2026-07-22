package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runtime-owned resource graph for a Yierdis instance.
 */
final class YierdisInstanceResources implements AutoCloseable {
    private final RuntimeDbEngine[] dbs;
    private final List<AutoCloseable> ownedResources;
    private final YierdisGlobalMaxmemoryGovernor globalMaxmemoryGovernor;
    private final CommitStream commitStream;

    YierdisInstanceResources(
            RuntimeDbEngine[] dbs,
            List<AutoCloseable> ownedResources,
            YierdisGlobalMaxmemoryGovernor globalMaxmemoryGovernor,
            CommitStream commitStream
    ) {
        Objects.requireNonNull(dbs, "dbs");
        this.dbs = dbs.clone();
        this.ownedResources = ownedResources == null ? List.of() : List.copyOf(ownedResources);
        this.globalMaxmemoryGovernor = globalMaxmemoryGovernor;
        this.commitStream = commitStream;
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

    CommitStream commitStream() {
        return commitStream;
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
        if (commitStream != null) {
            try {
                if (!commitStream.shutdown()) {
                    failure = recordFailure(failure, new IllegalStateException("commit stream did not drain"));
                }
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
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

        failure = closeOwnedResources(failure);

        rethrowIfNeeded(failure);
    }

    @Override
    public void close() {
        shutdownAll();
    }

    static RuntimeException startupFailure(
            Throwable failure,
            RuntimeDbEngine[] dbs,
            CommitStream commitStream,
            List<AutoCloseable> ownedResources
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
        if (commitStream != null) {
            try {
                commitStream.close();
            } catch (Throwable t) {
                cleanupFailure = recordFailure(cleanupFailure, t);
            }
        }
        cleanupFailure = closeOwnedResources(cleanupFailure, ownedResources);
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

    private Throwable closeOwnedResources(Throwable failure) {
        return closeOwnedResources(failure, ownedResources);
    }

    private static Throwable closeOwnedResources(Throwable failure, List<AutoCloseable> resources) {
        if (resources == null || resources.isEmpty()) {
            return failure;
        }
        List<AutoCloseable> reversed = new ArrayList<>(resources);
        for (int i = reversed.size() - 1; i >= 0; i--) {
            AutoCloseable resource = reversed.get(i);
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        return failure;
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
