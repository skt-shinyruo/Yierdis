package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;

/**
 * 根据原始请求计算命令回复上界，不访问会话或数据库状态。
 *
 * <p>该计划会在 {@code EXEC} 执行任何排队命令前聚合，因此实现必须是无副作用且只依赖请求内容的。
 * 返回的上界必须同时覆盖成功回复和命令运行期错误；无法证明上界时返回 {@link ReplyPlan#maximum()}。</p>
 */
@FunctionalInterface
public interface CommandReplyPlanner {
    ReplyPlan plan(ExecutionRequest request);
}
