package yier.bubu.redis.ops;

// StringWriteOps：string 写能力边界。

import yier.bubu.redis.bytes.BytesSlice;

public interface StringWriteOps {
    SetStringResult set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue);

    boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    long append(byte[] keyBytes, BytesSlice value);

    int setBit(byte[] keyBytes, long offset, int value);

    long incrBy(byte[] keyBytes, long delta);

    final class SetStringResult {
        private final boolean applied;
        private final byte[] oldValue;

        private SetStringResult(boolean applied, byte[] oldValue) {
            this.applied = applied;
            this.oldValue = oldValue;
        }

        public static SetStringResult of(boolean applied, byte[] oldValue) {
            return new SetStringResult(applied, oldValue);
        }

        public boolean applied() {
            return applied;
        }

        public byte[] oldValue() {
            return oldValue;
        }
    }
}
