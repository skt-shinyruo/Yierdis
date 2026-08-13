package yier.bubu.redis.command.defaults;

import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.common.command.MutationContext;

import java.util.Objects;

/**
 * Command-facing view of the selected DB capabilities used by built-in command handlers.
 */
public final class CommandDb {
    private final DbEngine engine;
    private final DbWrites writes;
    private final DbLifecycleOps lifecycle;

    CommandDb(DbEngine engine, MutationContext mutationContext) {
        this.engine = Objects.requireNonNull(engine, "engine");
        DbWrites engineWrites = engine.writes();
        DbLifecycleOps engineLifecycle = engine.lifecycle();
        this.writes = mutationContext == null
                ? engineWrites
                : engineWrites.withMutationContext(mutationContext);
        this.lifecycle = mutationContext == null
                ? engineLifecycle
                : engineLifecycle.withMutationContext(mutationContext);
    }

    public DbReads reads() {
        return engine.reads();
    }

    public DbWrites writes() {
        return writes;
    }

    public MemoryOps memory() {
        return engine.memory();
    }

    public DbLifecycleOps lifecycle() {
        return lifecycle;
    }
}
