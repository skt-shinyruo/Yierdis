package yier.bubu.redis.execution.api;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Immutable heap-backed {@link ExecutionRequest}.
 */
public final class ByteArrayExecutionRequest implements ExecutionRequest {
    private final byte[][] argv;
    private final int retainedBytes;
    private final boolean exposeReadOnlyBacking;
    private final RequestMemoryLease lease;

    private ByteArrayExecutionRequest(byte[][] argv, int retainedBytes, boolean exposeReadOnlyBacking) {
        this(argv, retainedBytes, exposeReadOnlyBacking, RequestMemoryLease.NOOP);
    }

    private ByteArrayExecutionRequest(
            byte[][] argv,
            int retainedBytes,
            boolean exposeReadOnlyBacking,
            RequestMemoryLease lease
    ) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
        this.exposeReadOnlyBacking = exposeReadOnlyBacking;
        this.lease = lease;
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
            retainedBytes = saturatedRetainedBytes(retainedBytes, copy.length);
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes, false);
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
            retainedBytes = saturatedRetainedBytes(retainedBytes, copy.length);
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes, false);
    }

    public static ByteArrayExecutionRequest wrapReadOnly(byte[][] argv, int retainedBytes) {
        Objects.requireNonNull(argv, "argv");
        return new ByteArrayExecutionRequest(argv.clone(), Math.max(0, retainedBytes), true);
    }

    public static ByteArrayExecutionRequest takeOwnership(
            byte[][] argv,
            int retainedBytes,
            RequestMemoryLease lease
    ) {
        Objects.requireNonNull(argv, "argv");
        Objects.requireNonNull(lease, "lease");
        return new ByteArrayExecutionRequest(
                argv,
                Math.max(0, retainedBytes),
                true,
                lease
        );
    }

    public static ByteArrayExecutionRequest fromUtf8(String commandName, List<String> args) {
        Objects.requireNonNull(commandName, "commandName");
        Objects.requireNonNull(args, "args");
        byte[][] argv = new byte[args.size() + 1][];
        int retainedBytes = 0;

        byte[] commandBytes = commandName.getBytes(StandardCharsets.UTF_8);
        argv[0] = commandBytes;
        retainedBytes = saturatedRetainedBytes(retainedBytes, commandBytes.length);

        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (arg == null) {
                continue;
            }
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            argv[i + 1] = bytes;
            retainedBytes = saturatedRetainedBytes(retainedBytes, bytes.length);
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes, true);
    }

    static int saturatedRetainedBytes(int retainedBytes, int argLength) {
        long next = (long) Math.max(0, retainedBytes) + Math.max(0, argLength);
        return next >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) next;
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
        byte[] arg = argv[index];
        if (arg == null) {
            return null;
        }
        return exposeReadOnlyBacking ? arg : arg.clone();
    }

    @Override
    public int retainedBytes() {
        return retainedBytes;
    }

    @Override
    public long admittedMemoryBytes() {
        return lease == RequestMemoryLease.NOOP ? estimatedMemoryBytes(argv) : lease.reservedBytes();
    }

    @Override
    public ByteArrayExecutionRequest retain() {
        return new ByteArrayExecutionRequest(
                argv,
                retainedBytes,
                exposeReadOnlyBacking,
                lease.retain()
        );
    }

    @Override
    public void close() {
        lease.close();
    }

    public static long estimatedMemoryBytes(byte[][] argv) {
        Objects.requireNonNull(argv, "argv");
        long total = saturatedAdd(48L, argv.length * 8L);
        for (byte[] arg : argv) {
            if (arg != null) {
                total = saturatedAdd(total, 16L);
                total = saturatedAdd(total, align8(arg.length));
            }
        }
        return total;
    }

    private static long align8(int length) {
        return ((long) length + 7L) & ~7L;
    }

    private static long saturatedAdd(long left, long right) {
        return left >= Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
