package yier.bubu.redis.execution.api;

@FunctionalInterface
public interface CapacityRegistration extends AutoCloseable {
    CapacityRegistration NONE = () -> { };

    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
