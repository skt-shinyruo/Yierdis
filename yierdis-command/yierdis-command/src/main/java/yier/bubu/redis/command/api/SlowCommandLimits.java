package yier.bubu.redis.command.api;

import java.util.concurrent.TimeUnit;

public record SlowCommandLimits(long keysTimeBudgetNanos, int keysMaxResults) {
    public static final SlowCommandLimits DEFAULT = new SlowCommandLimits(
            TimeUnit.MILLISECONDS.toNanos(20),
            Integer.MAX_VALUE
    );
}
