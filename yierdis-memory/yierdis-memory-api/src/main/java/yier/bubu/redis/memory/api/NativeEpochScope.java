package yier.bubu.redis.memory.api;

public interface NativeEpochScope extends AutoCloseable {
    NativeEpochKind kind();

    long epoch();

    @Override
    void close();
}
