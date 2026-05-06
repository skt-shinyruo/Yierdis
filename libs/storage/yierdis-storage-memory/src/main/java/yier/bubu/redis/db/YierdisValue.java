package yier.bubu.redis.db;


import yier.bubu.redis.ops.ValueType;
interface YierdisValue extends AutoCloseable {
    ValueType type();

    ValueEncoding encoding();

    @Override
    void close();
}
