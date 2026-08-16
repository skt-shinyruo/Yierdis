package yier.bubu.redis.storage.api;

public interface PreparedMutation<R> extends AutoCloseable {
    R preview();

    boolean isCurrent();

    MutationOutcome commit();

    @Override
    void close();
}
