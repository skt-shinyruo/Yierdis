package yier.bubu.redis.protocol;

import java.util.Arrays;

/**
 * A decoded Redis command represented as an argv-style array of bulk string slices.
 * <p>
 * This is a deliberately minimal structure optimized for low allocation.
 */
public final class RespCommand implements AutoCloseable {
    private static final int DEFAULT_ARGV_CAPACITY = 16;

    private RespFrame frame;
    private int[] argOffsets;
    private int[] argLengths;
    private int argc;

    private RespCommand() {
        this.argOffsets = new int[DEFAULT_ARGV_CAPACITY];
        this.argLengths = new int[DEFAULT_ARGV_CAPACITY];
    }

    static RespCommand acquire(int argc) {
        RespCommand cmd = new RespCommand();
        cmd.ensureCapacity(argc);
        cmd.argc = argc;
        return cmd;
    }

    void setFrame(RespFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException("frame must not be null");
        }
        this.frame = frame;
    }

    public int argc() {
        return argc;
    }

    public boolean isNull(int index) {
        if (index < 0 || index >= argc) {
            throw new IndexOutOfBoundsException();
        }
        return argLengths[index] < 0;
    }

    public int len(int index) {
        if (index < 0 || index >= argc) {
            throw new IndexOutOfBoundsException();
        }
        return argLengths[index];
    }

    public byte byteAt(int index, int offset) {
        int len = len(index);
        if (len < 0) {
            throw new IllegalStateException("arg is null");
        }
        if (offset < 0 || offset >= len) {
            throw new IndexOutOfBoundsException();
        }
        return frame.getByte(argOffsets[index] + offset);
    }

    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        int len = len(index);
        if (len < 0) {
            throw new IllegalStateException("arg is null");
        }
        if (dstOff < 0 || dstOff + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        frame.getBytes(argOffsets[index], dst, dstOff, len);
    }

    public byte[] toByteArray(int index) {
        int len = len(index);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] out = new byte[len];
        frame.getBytes(argOffsets[index], out, 0, len);
        return out;
    }

    /**
     * Returns the backing frame buffer for this command.
     * <p>
     * The returned buffer is owned by this {@link RespCommand} and will be released on {@link #recycle()}.
     * Callers MUST NOT retain it beyond the command execution.
     */
    public RespFrame frame() {
        return frame;
    }

    /**
     * Returns the argument's start offset within {@link #frame()}.
     */
    public int argOffset(int index) {
        if (index < 0 || index >= argc) {
            throw new IndexOutOfBoundsException();
        }
        return argOffsets[index];
    }

    /**
     * Returns the backing argument metadata arrays. Only the first {@link #argc()} entries are valid.
     */
    int[] argOffsetsUnsafe() {
        return argOffsets;
    }

    int[] argLengthsUnsafe() {
        return argLengths;
    }

    RespFrame frameUnsafe() {
        return frame;
    }

    public void recycle() {
        if (frame != null) {
            frame.close();
            frame = null;
        }
        Arrays.fill(argOffsets, 0, argc, 0);
        Arrays.fill(argLengths, 0, argc, 0);
        argc = 0;
    }

    @Override
    public void close() {
        recycle();
    }

    private void ensureCapacity(int desired) {
        if (argOffsets.length >= desired) {
            return;
        }
        int next = argOffsets.length;
        while (next < desired) {
            next <<= 1;
        }
        argOffsets = Arrays.copyOf(argOffsets, next);
        argLengths = Arrays.copyOf(argLengths, next);
    }

    void setArgSlice(int index, int offset, int len) {
        argOffsets[index] = offset;
        argLengths[index] = len;
    }

    void setArgNull(int index) {
        argOffsets[index] = 0;
        argLengths[index] = -1;
    }
}
