package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.common.command.MutationContext;

import java.util.Objects;
import java.util.function.Function;

public final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final Runnable threadChecker;
    private final Function<MutationContext, MutationOutcome> flushDb;

    YierdisDbLifecycleOps(
            Runnable threadChecker,
            Function<MutationContext, MutationOutcome> flushDb
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.flushDb = Objects.requireNonNull(flushDb, "flushDb");
    }

    @Override
    public MutationOutcome flushDb() {
        threadChecker.run();
        return flushDb.apply(MutationContext.none());
    }

    @Override
    public DbLifecycleOps withMutationContext(MutationContext context) {
        threadChecker.run();
        return new ContextualLifecycleOps(Objects.requireNonNull(context, "context"));
    }

    private final class ContextualLifecycleOps implements DbLifecycleOps {
        private final MutationContext context;

        private ContextualLifecycleOps(MutationContext context) {
            this.context = context;
        }

        @Override
        public MutationOutcome flushDb() {
            threadChecker.run();
            return flushDb.apply(context);
        }

        @Override
        public DbLifecycleOps withMutationContext(MutationContext context) {
            threadChecker.run();
            return new ContextualLifecycleOps(Objects.requireNonNull(context, "context"));
        }
    }
}
