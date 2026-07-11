package yier.bubu.redis.memory.api;

public final class NativeCapacityExceededException extends OffHeapOutOfMemoryException {
    public NativeCapacityExceededException(String message) {
        super(message);
    }

    public NativeCapacityExceededException(String message, Throwable cause) {
        super(message);
        initCause(cause);
    }
}
