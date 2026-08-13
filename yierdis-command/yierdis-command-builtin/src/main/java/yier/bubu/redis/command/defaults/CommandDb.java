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
    private final MutationContext mutationContext;

    CommandDb(DbEngine engine, MutationContext mutationContext) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.mutationContext = mutationContext;
    }

    public DbReads reads() {
        return engine.reads();
    }

    public DbWrites writes() {
        DbWrites writes = engine.writes();
        return mutationContext == null ? writes : writes.withMutationContext(mutationContext);
    }

    public MemoryOps memory() {
        return engine.memory();
    }

    public DbLifecycleOps lifecycle() {
        DbLifecycleOps lifecycle = engine.lifecycle();
        return mutationContext == null ? lifecycle : lifecycle.withMutationContext(mutationContext);
    }
}
