package yier.bubu.redis.ops;

/**
 * Maxmemory coordination boundary.
 * <p>
 * Runtime can provide a global coordinator to engines so that maxmemory
 * enforcement is centralized and policy-consistent.
 */
public interface MaxmemoryCoordinator {
    void prepareWrite(long estimatedExtraBytes);

    long nextLruClock();
}

