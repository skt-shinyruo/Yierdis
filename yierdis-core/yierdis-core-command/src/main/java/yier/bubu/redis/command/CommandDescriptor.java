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
}
