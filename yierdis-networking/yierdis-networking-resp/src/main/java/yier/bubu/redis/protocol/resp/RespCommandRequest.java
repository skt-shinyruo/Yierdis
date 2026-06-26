package yier.bubu.redis.protocol.resp;

import java.util.List;
import java.util.Objects;

public final class RespCommandRequest {
    private final byte[][] argv;
    private final int retainedBytes;

    private RespCommandRequest(byte[][] argv, int retainedBytes) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
    }

    public static RespCommandRequest copyOf(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        byte[][] argv = new byte[args.size()][];
        int retainedBytes = 0;
        for (int i = 0; i < args.size(); i++) {
            byte[] arg = args.get(i);
            if (arg == null) {
                argv[i] = null;
                continue;
            }
            argv[i] = arg.clone();
            retainedBytes = saturatedRetainedBytes(retainedBytes, arg.length);
        }
        return new RespCommandRequest(argv, retainedBytes);
    }

    static int saturatedRetainedBytes(int retainedBytes, int argLength) {
        long next = (long) Math.max(0, retainedBytes) + Math.max(0, argLength);
        return next >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) next;
    }

    /**
     * Wraps already-owned argument bytes. Callers must treat inner arrays as immutable after passing them here.
     */
    public static RespCommandRequest wrapReadOnly(byte[][] argv, int retainedBytes) {
        Objects.requireNonNull(argv, "argv");
        byte[][] owned = new byte[argv.length][];
        for (int i = 0; i < argv.length; i++) {
            owned[i] = argv[i];
        }
        return new RespCommandRequest(owned, Math.max(0, retainedBytes));
    }

    public int argc() {
        return argv.length;
    }

    /**
     * Returns the stored argument bytes by read-only convention; callers must not mutate the returned array.
     */
    public byte[] readOnlyArg(int index) {
        return argv[index];
    }

    public int retainedBytes() {
        return retainedBytes;
    }
}
