package yier.bubu.redis.protocol;

/**
 * RESP value (RESP2 / RESP3).
 * <p>
 * This project uses {@link RespObject} primarily for:
 * <ul>
 *     <li>CLI / tests / debugging (object tree)</li>
 *     <li>Netty codec adapter output (see protocol-netty {@code RespEncoder})</li>
 * </ul>
 * <p>
 * The server hot path writes replies via {@link RespWriter} to avoid building object trees.
 * <p>
 * Supported types are defined by {@link RespType} (covers the RESP2 core types and a minimal RESP3 subset).
 */
public interface RespObject {
    RespType type();

    /**
     * Best-effort debugging / logging representation.
     * Not meant to be round-trippable or protocol-correct.
     */
    String toHumanReadableString();
}
