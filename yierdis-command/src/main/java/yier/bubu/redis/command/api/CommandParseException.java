package yier.bubu.redis.command.api;

/**
 * 命令 parse 失败。默认在 MULTI 里先 {@link MultiPolicy#QUEUE}；arity / 未知子命令用
 * {@link #aborting(String)}，与 Redis 容器命令 lookup 失败一样整笔入队拒绝。
 */
public final class CommandParseException extends RuntimeException {
    public enum MultiPolicy {
        /** 事务内 QUEUED，EXEC 时只有这一条失败。 */
        QUEUE,
        /** 事务内拒绝入队并整笔作废。 */
        ABORT
    }

    private final MultiPolicy multiPolicy;

    public CommandParseException(String replyMessage) {
        this(replyMessage, MultiPolicy.QUEUE);
    }

    public CommandParseException(String replyMessage, MultiPolicy multiPolicy) {
        super(java.util.Objects.requireNonNull(replyMessage, "replyMessage"));
        this.multiPolicy = java.util.Objects.requireNonNull(multiPolicy, "multiPolicy");
    }

    public static CommandParseException aborting(String replyMessage) {
        return new CommandParseException(replyMessage, MultiPolicy.ABORT);
    }

    public boolean abortsMulti() {
        return multiPolicy == MultiPolicy.ABORT;
    }
}
