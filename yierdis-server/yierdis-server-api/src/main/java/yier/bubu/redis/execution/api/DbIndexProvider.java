package yier.bubu.redis.execution.api;

/**
 * DB index provider used by routing logic without depending on a wider session interface.
 */
public interface DbIndexProvider {
    int dbIndex();
}

