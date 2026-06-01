package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

final class YierdisDbMemoryBudgetCallbacks {
    // 构造期打破 ledger -> expiration/maxmemory -> keyLifecycle 的依赖环；
    // 未绑定前故意 fail-fast，防止装配顺序变动时把预算检查静默降级。
    private Runnable cleanupExpired = unboundCleanupExpired();
    private LongConsumer evictUntilUnder = unboundEvictUntilUnder();
    private LongSupplier usedBytesForMaxmemory = unboundUsedBytesForMaxmemory();

    void bind(
            Runnable cleanupExpired,
            LongConsumer evictUntilUnder,
            LongSupplier usedBytesForMaxmemory
    ) {
        this.cleanupExpired = Objects.requireNonNull(cleanupExpired, "cleanupExpired");
        this.evictUntilUnder = Objects.requireNonNull(evictUntilUnder, "evictUntilUnder");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
    }

    void cleanupExpired() {
        cleanupExpired.run();
    }

    void evictUntilUnder(long limitBytes) {
        evictUntilUnder.accept(limitBytes);
    }

    long usedBytesForMaxmemory() {
        return usedBytesForMaxmemory.getAsLong();
    }

    private static Runnable unboundCleanupExpired() {
        return () -> {
            throw new IllegalStateException("cleanupExpired callback is not bound");
        };
    }

    private static LongConsumer unboundEvictUntilUnder() {
        return ignored -> {
            throw new IllegalStateException("evictUntilUnder callback is not bound");
        };
    }

    private static LongSupplier unboundUsedBytesForMaxmemory() {
        return () -> {
            throw new IllegalStateException("usedBytesForMaxmemory callback is not bound");
        };
    }
}
