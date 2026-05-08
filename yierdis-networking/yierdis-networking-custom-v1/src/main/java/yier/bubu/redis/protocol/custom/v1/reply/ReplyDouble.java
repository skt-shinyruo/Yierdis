package yier.bubu.redis.protocol.custom.v1.reply;

/**
 * 协议侧 double 值（必须为 finite）。
 */
public record ReplyDouble(double value) implements ReplyValue {
    public ReplyDouble {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("double must be finite");
        }
    }
}
