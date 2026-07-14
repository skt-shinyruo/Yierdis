package yier.bubu.redis.common.command;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 堆数组命令记录的引用计数所有者。
 *
 * <p>构造时深拷贝 argv。保留视图只增加共享所有权，不复制字节。</p>
 */
public final class ByteArrayCommandRecord implements ImmutableCommandRecord {
    private final SharedArgv shared;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ByteArrayCommandRecord(SharedArgv shared) {
        this.shared = shared;
    }

    public static ByteArrayCommandRecord copyOf(byte[]... argv) {
        Objects.requireNonNull(argv, "argv");
        byte[][] copied = new byte[argv.length][];
        for (int index = 0; index < argv.length; index++) {
            byte[] value = argv[index];
            copied[index] = value == null ? null : value.clone();
        }
        return new ByteArrayCommandRecord(new SharedArgv(copied));
    }

    @Override
    public int argc() {
        return values().length;
    }

    @Override
    public boolean isNull(int index) {
        return values()[index] == null;
    }

    @Override
    public int len(int index) {
        byte[] value = values()[index];
        return value == null ? -1 : value.length;
    }

    @Override
    public byte byteAt(int index, int offset) {
        byte[] value = requireValue(index);
        return value[offset];
    }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        Objects.requireNonNull(dst, "dst");
        byte[] value = requireValue(index);
        System.arraycopy(value, 0, dst, dstOff, value.length);
    }

    @Override
    public long retainedMemoryBytes() {
        ensureOpen();
        return shared.retainedMemoryBytes;
    }

    @Override
    public ByteArrayCommandRecord retain() {
        ensureOpen();
        shared.retain();
        return new ByteArrayCommandRecord(shared);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            shared.release();
        }
    }

    private byte[] requireValue(int index) {
        byte[] value = values()[index];
        if (value == null) {
            throw new IllegalStateException("command argument is null");
        }
        return value;
    }

    private byte[][] values() {
        ensureOpen();
        byte[][] values = shared.values;
        if (values == null) {
            throw new IllegalStateException("command record is released");
        }
        return values;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("command record view is closed");
        }
    }

    private static final class SharedArgv {
        private final AtomicInteger references = new AtomicInteger(1);
        private final long retainedMemoryBytes;
        private volatile byte[][] values;

        private SharedArgv(byte[][] values) {
            this.values = values;
            this.retainedMemoryBytes = estimateRetainedMemoryBytes(values);
        }

        private void retain() {
            for (;;) {
                int current = references.get();
                if (current <= 0) {
                    throw new IllegalStateException("command record is already released");
                }
                if (current == Integer.MAX_VALUE) {
                    throw new IllegalStateException("command record reference count overflow");
                }
                if (references.compareAndSet(current, current + 1)) {
                    return;
                }
            }
        }

        private void release() {
            int remaining = references.decrementAndGet();
            if (remaining < 0) {
                throw new IllegalStateException("command record reference count underflow");
            }
            if (remaining == 0) {
                values = null;
            }
        }

        private static long estimateRetainedMemoryBytes(byte[][] argv) {
            long total = 48L;
            total = addSaturating(total, multiplySaturating(argv.length, 8L));
            for (byte[] value : argv) {
                if (value == null) {
                    continue;
                }
                total = addSaturating(total, 16L);
                total = addSaturating(total, align8(value.length));
            }
            return total;
        }

        private static long align8(int length) {
            long value = Math.max(0, length);
            return (value + 7L) & ~7L;
        }

        private static long addSaturating(long left, long right) {
            return left >= Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
        }

        private static long multiplySaturating(long left, long right) {
            if (left <= 0L || right <= 0L) {
                return 0L;
            }
            return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
        }
    }
}
