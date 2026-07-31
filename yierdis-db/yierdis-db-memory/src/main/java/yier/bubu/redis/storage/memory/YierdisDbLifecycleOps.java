package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.Function;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.MutationOutcome;

public final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final Runnable threadChecker;
    private final Function<MutationContext, MutationOutcome> flushDb;
    private final MutationContext context;

    YierdisDbLifecycleOps(
            Runnable threadChecker,
            Function<MutationContext, MutationOutcome> flushDb
    ) {
        this(threadChecker, flushDb, MutationContext.none());
    }

    private YierdisDbLifecycleOps(
            Runnable threadChecker,
            Function<MutationContext, MutationOutcome> flushDb,
            MutationContext context
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.flushDb = Objects.requireNonNull(flushDb, "flushDb");
        this.context = Objects.requireNonNull(context, "context");
    }

    @Override
    public MutationOutcome flushDb() {
        threadChecker.run();
        return flushDb.apply(context);
    }

    @Override
    public DbLifecycleOps withMutationContext(MutationContext context) {
        threadChecker.run();
        return new YierdisDbLifecycleOps(
                threadChecker,
                flushDb,
                Objects.requireNonNull(context, "context")
        );
    }
}
