package yier.bubu.redis.memory.api;

public final class StaleNativeHandleException extends NativeMemoryException {
    public StaleNativeHandleException(String message) {
        super(message);
    }
}
