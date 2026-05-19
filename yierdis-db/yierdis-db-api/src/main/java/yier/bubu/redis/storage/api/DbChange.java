package yier.bubu.redis.storage.api;

import java.util.Objects;

public record DbChange(int dbIndex, DbChangeKind kind, byte[][] commandArgv) {
    public DbChange {
        kind = Objects.requireNonNull(kind, "kind");
        commandArgv = copy(commandArgv);
    }

    public static DbChange syntheticDelete(int dbIndex, DbChangeKind kind, byte[] key) {
        Objects.requireNonNull(key, "key");
        return new DbChange(dbIndex, kind, new byte[][]{
                new byte[]{'D', 'E', 'L'},
                key
        });
    }

    @Override
    public byte[][] commandArgv() {
        return copy(commandArgv);
    }

    private static byte[][] copy(byte[][] source) {
        Objects.requireNonNull(source, "commandArgv");
        byte[][] out = new byte[source.length][];
        for (int i = 0; i < source.length; i++) {
            byte[] arg = source[i];
            out[i] = arg == null ? null : arg.clone();
        }
        return out;
    }
}
