package yier.bubu.redis.memory.api;

public class NativeMemoryException extends RuntimeException {
    public NativeMemoryException(String message) {
        super(message);
    }

    public NativeMemoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
