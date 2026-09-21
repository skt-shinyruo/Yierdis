package yier.bubu.redis.storage.api;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.result.ByteValue;

public interface StringOps {
    ByteValue getStringValue(BytesView keyView);

    long strlen(BytesView keyView);

    int getBit(BytesView keyView, long offset);

    long bitcount(BytesView keyView);

    long bitcount(BytesView keyView, long start, long end);

    WriteResult<SetStringValue> set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    PreparedMutation<SetStringValue> prepareSet(
            byte[] keyBytes,
            BytesSlice value,
            SetMode mode,
            ExpireOption expireOption,
            boolean returnOldValue
    );

    WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    WriteResult<Long> append(byte[] keyBytes, BytesSlice value);

    WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value);

    WriteResult<Long> incrBy(byte[] keyBytes, long delta);

    record SetStringValue(boolean applied, ByteValue oldValue) implements AutoCloseable {
        public SetStringValue {
            oldValue = oldValue == null ? ByteValue.NULL : oldValue;
        }

        @Override
        public void close() {
            oldValue.close();
        }
    }
}
