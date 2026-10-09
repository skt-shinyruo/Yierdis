package yier.bubu.redis.storage.api;

// BitRangeUnit：BITCOUNT start/end 的下标单位，对应 Redis 的 BYTE（默认）与 BIT 选项。

public enum BitRangeUnit {
    BYTE,
    BIT
}
