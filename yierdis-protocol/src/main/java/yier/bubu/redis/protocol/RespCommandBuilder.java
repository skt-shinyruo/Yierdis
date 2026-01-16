package yier.bubu.redis.protocol;

/**
 * Construction helpers for {@link RespCommand} used by protocol codecs.
 * <p>
 * This class exists to keep {@link RespCommand}'s mutation APIs package-private while allowing
 * codec implementations to live in sub-packages (e.g. Netty adapters).
 */
public final class RespCommandBuilder {
    private RespCommandBuilder() {
    }

    public static RespCommand acquire(int argc) {
        return RespCommand.acquire(argc);
    }

    public static void setFrame(RespCommand cmd, RespFrame frame) {
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
        cmd.setFrame(frame);
    }

    /**
     * Replaces the backing frame for the given command.
     * <p>
     * This method will close the existing frame (if any) to avoid leaking buffers, then set the new frame.
     * The argv offsets/lengths remain unchanged, so callers MUST ensure the new frame contains the same bytes
     * as the old frame.
     */
    public static void replaceFrame(RespCommand cmd, RespFrame newFrame) {
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
        if (newFrame == null) {
            throw new IllegalArgumentException("newFrame must not be null");
        }
        RespFrame old = cmd.frameUnsafe();
        if (old != null) {
            old.close();
        }
        cmd.setFrame(newFrame);
    }

    public static void setArgSlice(RespCommand cmd, int index, int offset, int len) {
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
        cmd.setArgSlice(index, offset, len);
    }

    public static void setArgNull(RespCommand cmd, int index) {
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
        cmd.setArgNull(index);
    }
}
