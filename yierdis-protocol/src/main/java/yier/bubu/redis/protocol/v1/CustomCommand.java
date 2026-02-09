package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.protocol.Command;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Custom protocol v1 command: {@code {"cmd":"...","args":[...]}} mapped to argv bytes.
 * <p>
 * Arguments are UTF-8 encoded. {@code null} args are allowed to represent a null bulk string.
 */
public final class CustomCommand implements Command {
    private final byte[][] argv;
    private final int retainedBytes;

    public CustomCommand(String cmd, List<String> args) {
        String name = Objects.requireNonNull(cmd, "cmd").trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("cmd must not be blank");
        }
        int argc = 1 + (args == null ? 0 : args.size());
        byte[][] a = new byte[argc][];
        a[0] = name.getBytes(StandardCharsets.UTF_8);

        int total = a[0].length;
        if (args != null && !args.isEmpty()) {
            for (int i = 0; i < args.size(); i++) {
                String s = args.get(i);
                if (s == null) {
                    a[i + 1] = null;
                    continue;
                }
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                a[i + 1] = bytes;
                total += bytes.length;
            }
        }
        this.argv = a;
        this.retainedBytes = total;
    }

    @Override
    public int argc() {
        return argv.length;
    }

    @Override
    public boolean isNull(int index) {
        if (index < 0 || index >= argv.length) {
            throw new IndexOutOfBoundsException();
        }
        return argv[index] == null;
    }

    @Override
    public int len(int index) {
        if (index < 0 || index >= argv.length) {
            throw new IndexOutOfBoundsException();
        }
        byte[] a = argv[index];
        return a == null ? -1 : a.length;
    }

    @Override
    public byte byteAt(int index, int offset) {
        byte[] a = argv[index];
        if (a == null) {
            throw new IllegalStateException("arg is null");
        }
        if (offset < 0 || offset >= a.length) {
            throw new IndexOutOfBoundsException();
        }
        return a[offset];
    }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        byte[] a = argv[index];
        if (a == null) {
            throw new IllegalStateException("arg is null");
        }
        if (dstOff < 0 || dstOff + a.length > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        if (a.length == 0) {
            return;
        }
        System.arraycopy(a, 0, dst, dstOff, a.length);
    }

    @Override
    public byte[] toByteArray(int index) {
        if (index < 0 || index >= argv.length) {
            throw new IndexOutOfBoundsException();
        }
        return argv[index];
    }

    @Override
    public int retainedBytes() {
        return retainedBytes;
    }

    @Override
    public void close() {
        // no-op (heap arrays will be GC'ed)
    }
}

