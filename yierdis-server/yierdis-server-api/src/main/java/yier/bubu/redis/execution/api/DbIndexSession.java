package yier.bubu.redis.execution.api;

/**
 * Connection-scoped DB selection state.
 */
public interface DbIndexSession extends DbIndexProvider {
    @Override
    int dbIndex();

    void setDbIndex(int dbIndex);
}
