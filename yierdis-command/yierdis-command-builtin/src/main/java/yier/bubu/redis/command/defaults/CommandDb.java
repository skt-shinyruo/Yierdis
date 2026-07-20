package yier.bubu.redis.command.defaults;

import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.MemoryOps;

import java.util.Objects;

/**
 * Command-facing view of the selected DB capabilities used by built-in command handlers.
 */
public final class CommandDb {
    private DbEngine engine;
    private DbWrites writes;
    private DbLifecycleOps lifecycle;

    CommandDb() {
    }

    CommandDb reset(DbEngine engine, yier.bubu.redis.common.command.MutationContext mutationContext) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.writes = engine.writes().withMutationContext(
                Objects.requireNonNull(mutationContext, "mutationContext")
        );
        this.lifecycle = engine.lifecycle().withMutationContext(mutationContext);
        return this;
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
