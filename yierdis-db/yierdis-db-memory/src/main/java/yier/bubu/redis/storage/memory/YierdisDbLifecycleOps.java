package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.Function;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.MutationOutcome;

public final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final Runnable threadChecker;
    private final Function<MutationContext, MutationOutcome> flushDb;
    private final Function<MutationContext, MutationOutcome> flushDbAsync;
    private final MutationContext context;

    YierdisDbLifecycleOps(
            Runnable threadChecker,
            Function<MutationContext, MutationOutcome> flushDb,
            Function<MutationContext, MutationOutcome> flushDbAsync
    ) {
        this(threadChecker, flushDb, flushDbAsync, MutationContext.none());
    }

    private YierdisDbLifecycleOps(
            Runnable threadChecker,
            Function<MutationContext, MutationOutcome> flushDb,
            Function<MutationContext, MutationOutcome> flushDbAsync,
            MutationContext context
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.flushDb = Objects.requireNonNull(flushDb, "flushDb");
        this.flushDbAsync = Objects.requireNonNull(flushDbAsync, "flushDbAsync");
        this.context = Objects.requireNonNull(context, "context");
    }

    @Override
    public MutationOutcome flushDb() {
        threadChecker.run();
        return flushDb.apply(context);
    }

    @Override
    public MutationOutcome flushDbAsync() {
        threadChecker.run();
        return flushDbAsync.apply(context);
    }

    @Override
    public DbLifecycleOps withMutationContext(MutationContext context) {
        threadChecker.run();
        return new YierdisDbLifecycleOps(
                threadChecker,
                flushDb,
                flushDbAsync,
                Objects.requireNonNull(context, "context")
        );
    }
}
