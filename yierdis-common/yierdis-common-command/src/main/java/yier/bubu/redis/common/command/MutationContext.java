package yier.bubu.redis.common.command;

import java.util.Objects;

/**
 * 一次命令执行向存储层传递的 mutation 元数据。
 *
 * <p>上下文只借用命令记录；命令执行边界结束后必须关闭上下文。需要跨越当前调用栈持有记录的消费者
 * 必须通过 {@link #retainCommandRecord()} 获取独立所有权。</p>
 */
public final class MutationContext implements AutoCloseable {
    private static final MutationContext NONE = new MutationContext(null);

    private ImmutableCommandRecord commandRecord;

    private MutationContext(ImmutableCommandRecord commandRecord) {
        this.commandRecord = commandRecord;
    }

    public static MutationContext none() {
        return NONE;
    }

    public static MutationContext of(ImmutableCommandRecord commandRecord) {
        return new MutationContext(Objects.requireNonNull(commandRecord, "commandRecord"));
    }

    public boolean hasCommandRecord() {
        return commandRecord != null;
    }

    public ImmutableCommandRecord commandRecord() {
        return commandRecord;
    }

    public ImmutableCommandRecord retainCommandRecord() {
        return commandRecord == null ? null : commandRecord.retain();
    }

    /**
     * 结束对命令记录的借用，但不关闭记录所有者。
     */
    @Override
    public void close() {
        if (this != NONE) {
            commandRecord = null;
        }
    }
}
