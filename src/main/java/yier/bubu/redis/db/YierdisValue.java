package yier.bubu.redis.db;

interface YierdisValue {
    ValueType type();

    ValueEncoding encoding();
}
