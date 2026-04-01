package yier.bubu.redis.protocol.reply;

import java.util.Arrays;
import java.util.Objects;

/**
 * 协议侧 bytes 值（bulk string 语义）。
 * <p>
 * 注意：该类型表达“字节序列”的语义，encoder 可以选择 UTF-8 string 或 base64 tagged value 的 wire 表示。
 */
public final class ReplyBytes implements ReplyValue {
    private final byte[] data;

    public ReplyBytes(byte[] data) {
        Objects.requireNonNull(data, "data");
        this.data = data;
    }

    public byte[] data() {
        return data;
    }

    public byte[] copy() {
        return Arrays.copyOf(data, data.length);
    }
}
