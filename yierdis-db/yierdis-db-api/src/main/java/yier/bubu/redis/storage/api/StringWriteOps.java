package yier.bubu.redis.storage.api;

// StringWriteOps：string 写能力边界。

import yier.bubu.redis.bytes.BytesSlice;

public interface StringWriteOps {
    WriteResult<SetStringValue> set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue);

    WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    WriteResult<Long> append(byte[] keyBytes, BytesSlice value);

    WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value);

    WriteResult<Long> incrBy(byte[] keyBytes, long delta);

    record SetStringValue(boolean applied, byte[] oldValue) {
    }
}
