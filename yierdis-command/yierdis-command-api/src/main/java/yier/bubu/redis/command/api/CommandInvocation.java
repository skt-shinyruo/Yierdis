package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;

@FunctionalInterface
public interface CommandInvocation {
    PreparedCommand prepare(CommandSession session);
}
