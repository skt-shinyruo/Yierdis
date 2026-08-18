package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.YierdisCommandException;

public final class YierdisDbHealth {
    public static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    private final Runnable threadChecker;
    private DbHealthSnapshot firstFailure = DbHealthSnapshot.healthy();

    public YierdisDbHealth(Runnable threadChecker) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
    }

    public DbHealthSnapshot snapshot() {
        threadChecker.run();
        return firstFailure;
    }

    public void requireWritable() {
        threadChecker.run();
        if (firstFailure.degraded()) {
            throw new YierdisCommandException(MISCONF_DEGRADED);
        }
    }

    public void recordInvariantFailure(Throwable failure) {
        threadChecker.run();
        Objects.requireNonNull(failure, "failure");
        if (firstFailure.degraded()) {
            return;
        }
        firstFailure = DbHealthSnapshot.degraded(failure, System.currentTimeMillis());
    }
}
