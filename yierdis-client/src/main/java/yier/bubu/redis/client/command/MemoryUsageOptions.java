package yier.bubu.redis.client.command;

import java.util.List;

/**
 * MEMORY USAGE 的 SAMPLES。采样个数不改变这台 server 的用量结果，但关键字仍会原样送出。
 */
public final class MemoryUsageOptions {
    private final long samples;

    private MemoryUsageOptions(long samples) {
        this.samples = samples;
    }

    public static MemoryUsageOptions samples(long samples) {
        return new MemoryUsageOptions(samples);
    }

    public List<String> tokens() {
        return List.of("SAMPLES", Long.toString(samples));
    }
}
