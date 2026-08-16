package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.Supplier;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.MutationOutcome;

final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final Runnable threadChecker;
    private final Supplier<MutationOutcome> flushDb;
    private final Supplier<MutationOutcome> flushDbAsync;

    YierdisDbLifecycleOps(
            Runnable threadChecker,
            Supplier<MutationOutcome> flushDb,
            Supplier<MutationOutcome> flushDbAsync
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.flushDb = Objects.requireNonNull(flushDb, "flushDb");
        this.flushDbAsync = Objects.requireNonNull(flushDbAsync, "flushDbAsync");
    }

    @Override
    public MutationOutcome flushDb() {
        threadChecker.run();
        return flushDb.get();
    }

    @Override
    public MutationOutcome flushDbAsync() {
        threadChecker.run();
        return flushDbAsync.get();
    }
}
