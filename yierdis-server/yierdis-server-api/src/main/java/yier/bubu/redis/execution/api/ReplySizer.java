package yier.bubu.redis.execution.api;

@FunctionalInterface
public interface ReplySizer {
    ReplyPlan plan(CommandSession session, ReplyShape shape);
}
