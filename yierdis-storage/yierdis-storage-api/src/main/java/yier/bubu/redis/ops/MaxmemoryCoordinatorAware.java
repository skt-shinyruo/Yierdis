package yier.bubu.redis.ops;

/**
 * SPI for attaching a global maxmemory coordinator to an engine.
 */
public interface MaxmemoryCoordinatorAware {
    /**
     * Attach or detach a global maxmemory coordinator.
     * <p>
     * Passing {@code null} detaches any previously attached coordinator.
     *
     * @param coordinator coordinator to attach, or {@code null} to detach
     */
    void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator);
}
