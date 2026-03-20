package yier.bubu.redis.ops;

// StringWriteOps：string 写能力边界。

import yier.bubu.redis.bytes.BytesSlice;

public interface StringWriteOps {
    boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    long append(byte[] keyBytes, BytesSlice value);

    int setBit(byte[] keyBytes, long offset, int value);

    long incrBy(byte[] keyBytes, long delta);
}
