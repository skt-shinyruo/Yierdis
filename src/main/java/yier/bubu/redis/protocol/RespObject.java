package yier.bubu.redis.protocol;

/**
 * RESP2 value.
 * <p>
 * Supported types:
 * + Simple String
 * - Error
 * : Integer
 * $ Bulk String (including $-1 nil)
 * * Array (including *-1 null array)
 */
public interface RespObject {
    RespType type();

    /**
     * Best-effort debugging / logging representation.
     * Not meant to be round-trippable or protocol-correct.
     */
    String toHumanReadableString();
}
