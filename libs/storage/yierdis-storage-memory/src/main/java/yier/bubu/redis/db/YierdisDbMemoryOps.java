package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.YierdisMemoryStats;

import java.util.Objects;

final class YierdisDbMemoryOps implements MemoryOps {
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
