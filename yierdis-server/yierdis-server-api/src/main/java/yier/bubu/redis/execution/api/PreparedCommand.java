package yier.bubu.redis.execution.api;

import java.util.OptionalInt;

/**
 * 容量预留前完成读取和校验、容量预留后执行一次的命令工作单元。
 *
 * <p>{@link #reservationShape()} 描述预留边界，不要求和最终结果形状相同。
 * {@link #execute(CommandSession)} 返回的回复可能引用该实例持有的资源，调用方必须在消费完结果后再关闭实例。</p>
 */
public interface PreparedCommand extends AutoCloseable {
    ReplyShape reservationShape();

    /**
     * 本回复 sizing 与渲染共同使用的 RESP 协议版本 wire value。
     *
     * <p>默认空，表示 prepare/预留时刻捕获 session 当前版本。在 execute 期才切换版本的命令
     * （HELLO）必须在 prepare 时声明协商后的目标版本，容量预留与实际写出才能按同一版本计算。</p>
     */
    default OptionalInt replyProtocolVersion() {
        return OptionalInt.empty();
    }

    ValidationResult validateBeforeExecute();

    CommandResult execute(CommandSession context);

    @Override
    void close();
}
