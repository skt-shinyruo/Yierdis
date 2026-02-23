package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisMemoryStats;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;

/**
 * Server 运行时信息提供者（可观测性扩展点）。
 * <p>
 * 该接口位于 core 模块，用于让命令层（INFO/STATS）在保持 core Netty-free 的前提下，
 * 仍能获取 server/executor/connection 的统计摘要。
 */
public interface ServerInfoProvider {
    void info(Command cmd, CommandContext ctx);

    void stats(Command cmd, CommandContext ctx);

    /**
     * 供 MEMORY STATS 等命令复用的内存摘要（可选）。
     * <p>
     * 返回 {@code null} 表示使用 DB 级默认实现（兼容单 DB 或不关心全局口径的场景）。
     */
    default YierdisMemoryStats memoryStats(CommandContext ctx) {
        return null;
    }
}
