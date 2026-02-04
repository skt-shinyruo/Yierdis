package yier.bubu.redis.ops;

// ValueOps：按 value 类型分组的操作集合（String/Hash/List/Set/ZSet/HLL）。

public interface ValueOps {
    StringOps strings();

    HashOps hashes();

    ListOps lists();

    SetOps sets();

    ZSetOps zsets();

    HllOps hll();
}

