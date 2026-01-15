package yier.bubu.redis.db;

interface YierdisValue extends AutoCloseable {
    ValueType type();

    ValueEncoding encoding();

    @Override
    void close();
}
