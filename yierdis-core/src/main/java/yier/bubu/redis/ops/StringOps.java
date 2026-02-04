package yier.bubu.redis.ops;

// StringOps：string 类型操作边界（set/get/append/strlen/bitops 等）。

import yier.bubu.redis.db.YierdisBulkStringOutput;
import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;

public interface StringOps {
    boolean setString(byte[] keyBytes, byte[] value, YierdisDb.SetMode mode, YierdisDb.ExpireOption expireOption);

    boolean setString(byte[] keyBytes, RespCommand cmd, int valueArgIndex, YierdisDb.SetMode mode, YierdisDb.ExpireOption expireOption);

    void getStringForReply(YierdisBytesView keyView, YierdisBulkStringOutput out);

    long strlen(YierdisBytesView keyView);

    long append(byte[] keyBytes, RespCommand cmd, int valueArgIndex);

    int setBit(byte[] keyBytes, long offset, int value);

    int getBit(YierdisBytesView keyView, long offset);

    long bitcount(YierdisBytesView keyView);

    long bitcount(YierdisBytesView keyView, long start, long end);

    long incrBy(byte[] keyBytes, long delta);
}
