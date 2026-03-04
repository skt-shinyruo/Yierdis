package yier.bubu.redis.ops;

/**
 * SPI for attaching a global maxmemory coordinator to an engine.
 */
public interface MaxmemoryCoordinatorAware {
    void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator);
}

