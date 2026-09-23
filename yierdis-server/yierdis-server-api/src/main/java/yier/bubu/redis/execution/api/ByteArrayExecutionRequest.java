package yier.bubu.redis.execution.api;

import yier.bubu.redis.common.memory.HeapRequestFootprint;

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
        for (int i = 0; i < args.size(); i++) {
            byte[] arg = args.get(i);
            if (arg == null) {
                continue;
            }
            argv[i] = arg.clone();
        }
        return new ByteArrayExecutionRequest(argv, HeapRequestFootprint.estimateRetainedBytes(argv), false);
    }

    public static ByteArrayExecutionRequest copyOf(ExecutionRequest request) {
        Objects.requireNonNull(request, "request");
        int argc = request.argc();
        byte[][] argv = new byte[argc][];
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
        }
        return new ByteArrayExecutionRequest(argv, HeapRequestFootprint.estimateRetainedBytes(argv), false);
    }

    public static ByteArrayExecutionRequest wrapReadOnly(byte[][] argv) {
        Objects.requireNonNull(argv, "argv");
        byte[][] copy = argv.clone();
        return new ByteArrayExecutionRequest(copy, HeapRequestFootprint.estimateRetainedBytes(copy), true);
    }

    public static ByteArrayExecutionRequest takeOwnership(byte[][] argv, RequestMemoryLease lease) {
        Objects.requireNonNull(argv, "argv");
        Objects.requireNonNull(lease, "lease");
        return new ByteArrayExecutionRequest(
                argv,
                HeapRequestFootprint.estimateRetainedBytes(argv),
                true,
                lease
        );
    }

    public static ByteArrayExecutionRequest fromUtf8(String commandName, List<String> args) {
        Objects.requireNonNull(commandName, "commandName");
        Objects.requireNonNull(args, "args");
        byte[][] argv = new byte[args.size() + 1][];
        argv[0] = commandName.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (arg == null) {
                continue;
            }
            argv[i + 1] = arg.getBytes(StandardCharsets.UTF_8);
        }
        return new ByteArrayExecutionRequest(argv, HeapRequestFootprint.estimateRetainedBytes(argv), true);
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

    /**
     * 请求视图保活期间占用的 heap footprint 估算，与 {@link HeapRequestFootprint} 同口径。
     */
    public static long estimatedMemoryBytes(byte[][] argv) {
        return HeapRequestFootprint.estimateBytes(argv);
    }
}
