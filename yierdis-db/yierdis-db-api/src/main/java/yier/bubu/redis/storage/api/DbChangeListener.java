package yier.bubu.redis.storage.api;

@FunctionalInterface
public interface DbChangeListener {
    DbChangeListener NOOP = change -> {
    };

    void onDbChange(DbChange change);
}
