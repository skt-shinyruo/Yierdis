package yier.bubu.redis.storage.api;

public interface RuntimeDbEngine extends DbEngine {
    void bindToCurrentThread();
    void runMaintenance();

    default void runDeferredReclamation() {
    }

    default void defragMaintenance() {
    }

    void shutdown();
}
