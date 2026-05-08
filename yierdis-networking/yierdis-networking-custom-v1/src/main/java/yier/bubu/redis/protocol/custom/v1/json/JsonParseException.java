package yier.bubu.redis.protocol.custom.v1.json;

/**
 * Thrown when a JSON input is invalid or exceeds configured limits.
 */
public final class JsonParseException extends IllegalArgumentException {
    public JsonParseException(String message) {
        super(message);
    }
}
