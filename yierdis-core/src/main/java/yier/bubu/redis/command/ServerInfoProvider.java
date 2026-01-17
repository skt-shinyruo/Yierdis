package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

/**
 * Server 运行时信息提供者（可观测性扩展点）。
 * <p>
 * 该接口位于 core 模块，用于让命令层（INFO/STATS）在保持 core Netty-free 的前提下，
 * 仍能获取 server/executor/connection 的统计摘要。
 */
public interface ServerInfoProvider {
    void info(RespCommand cmd, RespWriter out);

    void stats(RespCommand cmd, RespWriter out);
}

