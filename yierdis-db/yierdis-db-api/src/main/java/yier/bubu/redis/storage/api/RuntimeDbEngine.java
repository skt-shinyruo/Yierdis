package yier.bubu.redis.storage.api;

public interface RuntimeDbEngine extends DbEngine {
    void bindToCurrentThread();
    void runMaintenance();
    void shutdown();
}
