package yier.bubu.redis.memory.api;

/**
 * 为指定 {@link MemoryOwner} 创建独立且尚未绑定的稳定内存后端。
 * 每次调用都返回非 {@code null} 的新后端；调用方接管后端及其关闭责任，工厂不得代为绑定 owner。
 */
@FunctionalInterface
public interface StableMemoryBackendFactory {
    StableMemoryBackend create(String name, int maxSlots, MemoryOwner owner);
}
