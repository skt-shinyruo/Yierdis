package yier.bubu.redis.command;

/**
 * Immutable COMMAND metadata descriptor.
 * <p>
 * Values map to RESP COMMAND INFO fields:
 * <pre>
 * [name, arity, flags, firstKey, lastKey, step]
 * </pre>
 */
public final class CommandDescriptor {
    private final int arity;
    private final int firstKeyIndex;
    private final int lastKeyIndex;
    private final int keyStep;

    private CommandDescriptor(int arity, int firstKeyIndex, int lastKeyIndex, int keyStep) {
        this.arity = arity;
        this.firstKeyIndex = firstKeyIndex;
        this.lastKeyIndex = lastKeyIndex;
        this.keyStep = keyStep;
    }

    public static CommandDescriptor of(int arity, int firstKeyIndex, int lastKeyIndex, int keyStep) {
        return new CommandDescriptor(arity, firstKeyIndex, lastKeyIndex, keyStep);
    }

    static CommandDescriptor defaultForNameUpper(String nameUpper) {
        return new CommandDescriptor(
                commandArity(nameUpper),
                firstKeyIndex(nameUpper),
                lastKeyIndex(nameUpper),
                keyStep(nameUpper)
        );
    }

    public int arity() {
        return arity;
    }

    public int firstKeyIndex() {
        return firstKeyIndex;
    }

    public int lastKeyIndex() {
        return lastKeyIndex;
    }

    public int keyStep() {
        return keyStep;
    }

    private static int commandArity(String nameUpper) {
        if (nameUpper == null) {
            return -1;
        }
        switch (nameUpper) {
            case "PING":
                return -1;
            case "ECHO":
                return 2;
            case "HELLO":
                return -1;
            case "COMMAND":
                return -1;
            case "INFO":
                return -1;
            case "STATS":
                return 1;
            case "SELECT":
                return 2;
            case "QUIT":
            case "FLUSHDB":
                return 1;
            case "TYPE":
            case "KEYS":
            case "TTL":
            case "GET":
            case "STRLEN":
            case "INCR":
            case "DECR":
            case "SMEMBERS":
            case "SCARD":
            case "HGETALL":
            case "HLEN":
                return 2;
            case "EXPIRE":
            case "APPEND":
            case "HGET":
            case "SISMEMBER":
            case "GETBIT":
                return 3;
            case "SETBIT":
            case "LRANGE":
            case "ZREMRANGEBYRANK":
            case "ZREMRANGEBYSCORE":
                return 4;
            case "DEL":
            case "EXISTS":
            case "MEMORY":
            case "OBJECT":
            case "BITCOUNT":
            case "LPOP":
            case "RPOP":
            case "PFCOUNT":
                return -2;
            case "SET":
            case "LPUSH":
            case "RPUSH":
            case "SADD":
            case "SREM":
            case "HDEL":
            case "ZREM":
            case "PFADD":
            case "PFMERGE":
                return -3;
            case "HSET":
            case "ZADD":
            case "ZRANGE":
            case "ZREVRANGE":
            case "ZRANGEBYSCORE":
            case "ZREVRANGEBYSCORE":
                return -4;
            default:
                return -1;
        }
    }

    private static int firstKeyIndex(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        switch (nameUpper) {
            case "PING":
            case "ECHO":
            case "HELLO":
            case "COMMAND":
            case "INFO":
            case "STATS":
            case "QUIT":
            case "FLUSHDB":
            case "SELECT":
            case "KEYS":
            case "MEMORY":
            case "OBJECT":
                return 0;
            default:
                return 1;
        }
    }

    private static int lastKeyIndex(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        switch (nameUpper) {
            case "DEL":
            case "EXISTS":
            case "PFCOUNT":
            case "PFMERGE":
                return -1;
            case "PING":
            case "ECHO":
            case "HELLO":
            case "COMMAND":
            case "INFO":
            case "STATS":
            case "SELECT":
            case "QUIT":
            case "FLUSHDB":
            case "KEYS":
            case "MEMORY":
            case "OBJECT":
                return 0;
            default:
                return 1;
        }
    }

    private static int keyStep(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        switch (nameUpper) {
            case "PING":
            case "ECHO":
            case "HELLO":
            case "COMMAND":
            case "INFO":
            case "STATS":
            case "QUIT":
            case "FLUSHDB":
            case "SELECT":
            case "KEYS":
            case "MEMORY":
            case "OBJECT":
                return 0;
            default:
                return 1;
        }
    }
}
