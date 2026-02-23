package yier.bubu.redis.protocol;

/**
 * DB index 提供者：用于路由在不依赖更宽的连接态能力时读取当前逻辑 DB 下标。
 */
public interface DbIndexProvider {
    int dbIndex();
}

