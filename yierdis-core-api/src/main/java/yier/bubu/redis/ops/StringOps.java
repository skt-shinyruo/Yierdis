package yier.bubu.redis.ops;

// StringOps：string 类型操作边界（set/get/append/strlen/bitops 等）。

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.result.BulkStringValue;

public interface StringOps {
    boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    /**
     * Returns the raw string bytes view of the value stored at {@code keyBytes}.
     * <p>
     * This is primarily used to implement command semantics such as {@code SET ... GET} without leaking DB types.
     *
     * @return {@code null} when the key does not exist or is expired.
     * @throws WrongTypeException when the key holds a non-string value.
     */
    byte[] getStringBytes(byte[] keyBytes);

    BulkStringValue getStringValue(BytesView keyView);

    long strlen(BytesView keyView);

    long append(byte[] keyBytes, BytesSlice value);

    int setBit(byte[] keyBytes, long offset, int value);

    int getBit(BytesView keyView, long offset);

    long bitcount(BytesView keyView);

    long bitcount(BytesView keyView, long start, long end);

    long incrBy(byte[] keyBytes, long delta);
}
