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
    private final DbEngine engine;

    CommandDb(DbEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine");
    }

    public DbReads reads() {
        return engine.reads();
    }

    public DbWrites writes() {
        return engine.writes();
    }

    public MemoryOps memory() {
        return engine.memory();
    }

    public DbLifecycleOps lifecycle() {
        return engine.lifecycle();
    }
}
