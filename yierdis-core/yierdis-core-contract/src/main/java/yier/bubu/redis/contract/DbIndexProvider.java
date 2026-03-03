package yier.bubu.redis.contract;

/**
 * DB index provider used by routing logic without depending on a wider session interface.
 */
public interface DbIndexProvider {
    int dbIndex();
}

