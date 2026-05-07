package yier.bubu.redis.storage.api;

// StringWriteOps：string 写能力边界。

import yier.bubu.redis.bytes.BytesSlice;

public interface StringWriteOps {
    SetStringResult set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue);

    WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    WriteResult<Long> append(byte[] keyBytes, BytesSlice value);

    WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value);

    WriteResult<Long> incrBy(byte[] keyBytes, long delta);

    final class SetStringResult {
        private final boolean applied;
        private final byte[] oldValue;
        private final MutationOutcome mutationOutcome;

        private SetStringResult(boolean applied, byte[] oldValue, MutationOutcome mutationOutcome) {
            this.applied = applied;
            this.oldValue = oldValue;
            this.mutationOutcome = mutationOutcome == null ? MutationOutcome.NONE : mutationOutcome;
        }

        public static SetStringResult of(boolean applied, byte[] oldValue) {
            return new SetStringResult(applied, oldValue, applied ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE);
        }

        public static SetStringResult of(boolean applied, byte[] oldValue, MutationOutcome mutationOutcome) {
            return new SetStringResult(applied, oldValue, mutationOutcome);
        }

        public boolean applied() {
            return applied;
        }

        public byte[] oldValue() {
            return oldValue;
        }

        public MutationOutcome mutationOutcome() {
            return mutationOutcome;
        }
    }
}
