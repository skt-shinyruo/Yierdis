package yier.bubu.redis.protocol.v1;

import java.util.Arrays;
import java.util.Objects;

/**
 * Custom Protocol v1 request model backed by already-decoded argument bytes.
 */
public final class CustomProtocolV1ArgvRequest {
    private final byte[][] argv;
    private final int retainedBytes;

    private CustomProtocolV1ArgvRequest(byte[][] argv, int retainedBytes) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
    }

    public static CustomProtocolV1ArgvRequest of(byte[][] argv, int retainedBytes) {
        Objects.requireNonNull(argv, "argv");
        return new CustomProtocolV1ArgvRequest(Arrays.copyOf(argv, argv.length), Math.max(0, retainedBytes));
    }

    public int argc() {
        return argv.length;
    }

    public boolean isNull(int index) {
        return argv[index] == null;
    }

    public byte[] readOnlyArg(int index) {
        return argv[index];
    }

    public int retainedBytes() {
        return retainedBytes;
    }
}
