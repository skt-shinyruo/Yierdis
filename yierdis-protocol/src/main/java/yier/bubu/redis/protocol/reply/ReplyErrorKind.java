package yier.bubu.redis.protocol.reply;

/**
 * Reply error kind taxonomy for Custom Protocol v1.
 * <p>
 * 注意：这是协议语义层的分类（SSOT），用于区分不同来源的错误并稳定 wire 表示。
 */
public enum ReplyErrorKind {
    PROTOCOL("protocol"),
    COMMAND("command"),
    INTERNAL("internal");

    private final String wireName;

    ReplyErrorKind(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }
}

