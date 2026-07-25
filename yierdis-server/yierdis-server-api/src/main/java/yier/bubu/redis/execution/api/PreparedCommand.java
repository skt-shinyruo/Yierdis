package yier.bubu.redis.execution.api;

/**
 * 容量预留前完成读取和校验、容量预留后执行一次的命令工作单元。
 */
public interface PreparedCommand extends AutoCloseable {
    ReplyShape replyShape();

    ValidationResult validateBeforeExecute();

    void execute(CommandExecutionContext context);

    @Override
    void close();
}
