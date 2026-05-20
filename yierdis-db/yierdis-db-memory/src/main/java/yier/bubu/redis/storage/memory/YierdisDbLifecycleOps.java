package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.MutationOutcome;

import java.util.Objects;
import java.util.function.Supplier;

public final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final Supplier<MutationOutcome> flushDb;

    YierdisDbLifecycleOps(Supplier<MutationOutcome> flushDb) {
        this.flushDb = Objects.requireNonNull(flushDb, "flushDb");
    }

    @Override
    public MutationOutcome flushDb() {
        return flushDb.get();
    }
}
