package yier.bubu.redis.storage.api;

@FunctionalInterface
public interface DbEngineFactory {
    RuntimeDbEngine create(DbEngineConfig config);
}
