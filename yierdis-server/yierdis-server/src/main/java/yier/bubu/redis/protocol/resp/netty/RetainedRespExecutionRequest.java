package yier.bubu.redis.protocol.resp.netty;

import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReferenceCountedRequestMemoryLease;
import yier.bubu.redis.execution.api.RequestMemoryLease;

import java.util.Objects;

/**
 * 由 RESP 解码器构造的不可变 argv 视图；保留副本共享相同数组和入站内存租约。
 */
final class RetainedRespExecutionRequest implements ExecutionRequest {
    private final byte[][] argv;
    private final int retainedBytes;
    private final RequestMemoryLease lease;

    private RetainedRespExecutionRequest(byte[][] argv, int retainedBytes, RequestMemoryLease lease) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
        this.lease = lease;
    }

    static RetainedRespExecutionRequest takeOwnership(byte[][] argv, int retainedBytes, RequestMemoryLease lease) {
        Objects.requireNonNull(argv, "argv");
        RequestMemoryLease ownedLease = lease == null
                ? new ReferenceCountedRequestMemoryLease(estimatedMemoryBytes(argv), ignored -> { })
                : lease;
        return new RetainedRespExecutionRequest(argv, Math.max(0, retainedBytes), ownedLease);
    }

    static long estimatedMemoryBytes(byte[][] argv) {
        long total = 48L;
        total = saturatedAdd(total, argv.length * 8L);
        for (byte[] arg : argv) {
            if (arg != null) {
                total = saturatedAdd(total, 16L);
                total = saturatedAdd(total, align8(arg.length));
            }
        }
        return total;
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
        byte[] arg = requireArg(index);
        return arg[offset];
    }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        byte[] arg = requireArg(index);
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
    public long admittedMemoryBytes() {
        return lease.reservedBytes();
    }

    @Override
    public RetainedRespExecutionRequest retain() {
        return new RetainedRespExecutionRequest(argv, retainedBytes, lease.retain());
    }

    @Override
    public void close() {
        lease.close();
    }

    private byte[] requireArg(int index) {
        byte[] arg = argv[index];
        if (arg == null) {
            throw new IllegalStateException("arg is null");
        }
        return arg;
    }

    private static long align8(int length) {
        return ((long) length + 7L) & ~7L;
    }

    private static long saturatedAdd(long left, long right) {
        return left >= Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
