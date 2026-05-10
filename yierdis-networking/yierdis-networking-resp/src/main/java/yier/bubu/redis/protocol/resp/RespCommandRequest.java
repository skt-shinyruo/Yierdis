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
                throw new IllegalArgumentException("RESP command argv must not contain null bulk strings");
            }
            argv[i] = arg.clone();
            retainedBytes += arg.length;
        }
        return new RespCommandRequest(argv, retainedBytes);
    }

    public static RespCommandRequest wrapReadOnly(byte[][] argv, int retainedBytes) {
        Objects.requireNonNull(argv, "argv");
        byte[][] owned = new byte[argv.length][];
        for (int i = 0; i < argv.length; i++) {
            if (argv[i] == null) {
                throw new IllegalArgumentException("RESP command argv must not contain null bulk strings");
            }
            owned[i] = argv[i];
        }
        return new RespCommandRequest(owned, Math.max(0, retainedBytes));
    }

    public int argc() {
        return argv.length;
    }

    public byte[] readOnlyArg(int index) {
        return argv[index];
    }

    public int retainedBytes() {
        return retainedBytes;
    }
}
