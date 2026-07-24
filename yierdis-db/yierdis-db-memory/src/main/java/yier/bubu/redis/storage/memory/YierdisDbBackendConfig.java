package yier.bubu.redis.storage.memory;

/**
 * 内存后端的实例级配置；每个 DB 的运行时策略由 {@code DbEngineConfig} 单独描述。
 */
public record YierdisDbBackendConfig(int nativeSlotCapacity) {
    public YierdisDbBackendConfig {
        if (nativeSlotCapacity < 0) {
            throw new IllegalArgumentException("nativeSlotCapacity must be non-negative");
        }
    }
}
