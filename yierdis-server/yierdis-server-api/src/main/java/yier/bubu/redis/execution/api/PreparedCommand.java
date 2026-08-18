package yier.bubu.redis.execution.api;

/**
 * 容量预留前完成读取和校验、容量预留后执行一次的命令工作单元。
 *
 * <p>{@link #reservationShape()} 描述预留边界，不要求和最终结果形状相同。
 * {@link #execute(CommandSession)} 返回的回复可能引用该实例持有的资源，调用方必须在消费完结果后再关闭实例。</p>
 */
public interface PreparedCommand extends AutoCloseable {
    ReplyShape reservationShape();

    ValidationResult validateBeforeExecute();

    CommandResult execute(CommandSession context);

    @Override
    void close();
}
