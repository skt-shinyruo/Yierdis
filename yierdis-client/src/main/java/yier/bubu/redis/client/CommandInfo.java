package yier.bubu.redis.client;

import java.util.List;

/**
 * COMMAND 列表里的一条。{@code commandInfo} 里服务端没有的名字是 null，不是这个记录。
 */
public record CommandInfo(
        String name,
        long arity,
        List<String> flags,
        long firstKey,
        long lastKey,
        long keyStep
) {
}
