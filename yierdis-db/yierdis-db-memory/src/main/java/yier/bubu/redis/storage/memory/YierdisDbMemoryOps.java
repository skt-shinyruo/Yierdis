package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.util.Objects;

public final class YierdisDbMemoryOps implements MemoryOps {
    private final YierdisDbMemoryReporter memoryReporter;
    private final YierdisDbIntrospection introspection;

    YierdisDbMemoryOps(YierdisDbMemoryReporter memoryReporter, YierdisDbIntrospection introspection) {
        this.memoryReporter = Objects.requireNonNull(memoryReporter, "memoryReporter");
        this.introspection = Objects.requireNonNull(introspection, "introspection");
    }

    @Override
    public long memoryUsage(BytesView keyView) {
        return memoryReporter.memoryUsage(keyView);
    }

    @Override
    public YierdisMemoryStats memoryStats() {
        return memoryReporter.memoryStats();
    }

    @Override
    public String objectEncoding(BytesView keyView) {
        return introspection.objectEncoding(keyView);
    }
}
