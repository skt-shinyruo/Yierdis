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

