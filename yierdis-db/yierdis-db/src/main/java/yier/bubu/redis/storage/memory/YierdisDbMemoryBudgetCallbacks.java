package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

final class YierdisDbMemoryBudgetCallbacks {
    // 构造期打破 ledger -> maxmemory -> keyLifecycle 的依赖环；
    // 未绑定前故意 fail-fast，防止装配顺序变动时把预算检查静默降级。
    private LongConsumer evictUntilUnder = unboundEvictUntilUnder();
    private LongSupplier usedBytesForMaxmemory = unboundUsedBytesForMaxmemory();

    void bind(
            LongConsumer evictUntilUnder,
            LongSupplier usedBytesForMaxmemory
    ) {
        this.evictUntilUnder = Objects.requireNonNull(evictUntilUnder, "evictUntilUnder");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
    }

    void evictUntilUnder(long limitBytes) {
        evictUntilUnder.accept(limitBytes);
    }

    long usedBytesForMaxmemory() {
        return usedBytesForMaxmemory.getAsLong();
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
