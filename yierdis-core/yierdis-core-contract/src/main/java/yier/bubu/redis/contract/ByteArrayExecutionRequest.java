package yier.bubu.redis.contract;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Immutable heap-backed {@link ExecutionRequest}.
 */
public final class ByteArrayExecutionRequest implements ExecutionRequest {
    private final byte[][] argv;
    private final int retainedBytes;

    private ByteArrayExecutionRequest(byte[][] argv, int retainedBytes) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
    }

    public static ByteArrayExecutionRequest copyOf(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        byte[][] argv = new byte[args.size()][];
        int retainedBytes = 0;
        for (int i = 0; i < args.size(); i++) {
            byte[] arg = args.get(i);
            if (arg == null) {
                continue;
            }
            byte[] copy = arg.clone();
            argv[i] = copy;
            retainedBytes += copy.length;
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes);
    }

    public static ByteArrayExecutionRequest copyOf(ExecutionRequest request) {
        Objects.requireNonNull(request, "request");
        int argc = request.argc();
        byte[][] argv = new byte[argc][];
        int retainedBytes = 0;
        for (int i = 0; i < argc; i++) {
            if (request.isNull(i)) {
                continue;
            }
            int len = request.len(i);
            if (len < 0) {
                continue;
            }
            byte[] copy = new byte[len];
            if (len > 0) {
                request.copyToByteArray(i, copy, 0);
            }
            argv[i] = copy;
            retainedBytes += copy.length;
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes);
    }

    public static ByteArrayExecutionRequest fromUtf8(String commandName, List<String> args) {
        Objects.requireNonNull(commandName, "commandName");
        Objects.requireNonNull(args, "args");
        byte[][] argv = new byte[args.size() + 1][];
        int retainedBytes = 0;

        byte[] commandBytes = commandName.getBytes(StandardCharsets.UTF_8);
        argv[0] = commandBytes;
        retainedBytes += commandBytes.length;

        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (arg == null) {
                continue;
            }
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            argv[i + 1] = bytes;
            retainedBytes += bytes.length;
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes);
    }

    @Override
    public int argc() {
        return argv.length;
    }

    @Override
    public boolean isNull(int index) {
        return argv[index] == null;
    }

    @Override
    public int len(int index) {
        byte[] arg = argv[index];
        return arg == null ? -1 : arg.length;
    }

    @Override
    public byte byteAt(int index, int offset) {
        byte[] arg = argv[index];
        if (arg == null) {
            throw new IllegalStateException("arg is null");
        }
        return arg[offset];
    }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        byte[] arg = argv[index];
        if (arg == null) {
            throw new IllegalStateException("arg is null");
        }
        System.arraycopy(arg, 0, dst, dstOff, arg.length);
    }

    @Override
    public byte[] toByteArray(int index) {
        byte[] arg = argv[index];
        return arg == null ? null : arg.clone();
    }

    @Override
    public byte[] readOnlyByteArray(int index) {
        return argv[index];
    }

    @Override
    public int retainedBytes() {
        return retainedBytes;
    }

    @Override
    public void close() {
        // heap-backed, nothing to release
    }
}
