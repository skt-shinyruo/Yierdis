package yier.bubu.redis.storage.api;

public interface DefragmentableDbEngine extends RuntimeDbEngine {
    void defragMaintenance();
}
