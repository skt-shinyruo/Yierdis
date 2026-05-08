package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;

import java.util.Objects;

public final class YierdisDbRuntimeInternals implements YierdisDbInternals {
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final MemoryLedger ledger;

    YierdisDbRuntimeInternals(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle,
            MemoryLedger ledger
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    @Override
    public void checkThread() {
        threadChecker.run();
    }

    @Override
    public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
        return mutationExecutor.execute(plan);
    }

    @Override
    public YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }

    @Override
    public MemoryLedger ledger() {
        return ledger;
    }
}
