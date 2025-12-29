package yier.bubu.redis.protocol;

import io.netty.util.Recycler;

import java.util.Arrays;

/**
 * A decoded Redis command represented as an argv-style array of bulk string bytes.
 * <p>
 * This is a deliberately minimal structure optimized for low allocation.
 */
public final class RespCommand {
    private static final Recycler<RespCommand> RECYCLER = new Recycler<>() {
        @Override
        protected RespCommand newObject(Handle<RespCommand> handle) {
            return new RespCommand(handle);
        }
    };

    private final Recycler.Handle<RespCommand> handle;

    private byte[][] argv;
    private int argc;

    private RespCommand(Recycler.Handle<RespCommand> handle) {
        this.handle = handle;
        this.argv = new byte[16][];
    }

    static RespCommand acquire(int argc) {
        RespCommand cmd = RECYCLER.get();
        cmd.ensureCapacity(argc);
        cmd.argc = argc;
        return cmd;
    }

    void setArg(int index, byte[] bytes) {
        argv[index] = bytes;
    }

    public int argc() {
        return argc;
    }

    public byte[] arg(int index) {
        if (index < 0 || index >= argc) {
            throw new IndexOutOfBoundsException();
        }
        return argv[index];
    }

    /**
     * Returns the backing argv array. Only the first {@link #argc()} entries are valid.
     */
    public byte[][] argvUnsafe() {
        return argv;
    }

    public void recycle() {
        Arrays.fill(argv, 0, argc, null);
        argc = 0;
        handle.recycle(this);
    }

    private void ensureCapacity(int desired) {
        if (argv.length >= desired) {
            return;
        }
        int next = argv.length;
        while (next < desired) {
            next <<= 1;
        }
        argv = Arrays.copyOf(argv, next);
    }
}
