package yier.bubu.redis.command;

// DB 路由抽象：将“命令执行时选择哪个 DB engine”与命令实现解耦，支持多 DB（SELECT）扩展。

import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.contract.ServerSession;

public interface YierdisDbRouter {
    /**
     * 根据当前连接/会话状态选择本次命令应作用的 DB。
     */
    DbEngine dbFor(ServerSession session);

    /**
     * 逻辑 DB 个数（用于 SELECT 参数校验与 INFO 输出）。
     */
    int databases();
}
