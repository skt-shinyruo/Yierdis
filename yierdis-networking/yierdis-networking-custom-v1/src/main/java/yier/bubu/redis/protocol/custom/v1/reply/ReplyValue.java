package yier.bubu.redis.protocol.custom.v1.reply;

import java.util.List;

/**
 * 协议侧 reply 值模型，供客户端、工具、解析器与协议编解码辅助逻辑使用。
 * <p>
 * 它不是 server 命令写回语义的单一事实来源；server 命令执行写回仍以 {@code ReplyWriter} 为准。
 * Custom Protocol v1 的 NDJSON encoder 可以将其映射到 wire 表示。
 */
public sealed interface ReplyValue permits ReplyNull, ReplyBoolean, ReplyLong, ReplyDouble, ReplyString, ReplyBytes, ReplyArray, ReplyMap, ReplyError {
    static ReplyNull nullValue() {
        return ReplyNull.INSTANCE;
    }

    static ReplyBoolean of(boolean v) {
        return new ReplyBoolean(v);
    }

    static ReplyLong of(long v) {
        return new ReplyLong(v);
    }

    static ReplyDouble of(double v) {
        return new ReplyDouble(v);
    }

    static ReplyString of(String v) {
        return new ReplyString(v);
    }

    static ReplyBytes bytes(byte[] data) {
        return new ReplyBytes(data);
    }

    static ReplyArray array(List<ReplyValue> values) {
        return new ReplyArray(values);
    }

    static ReplyMap map(List<ReplyMap.Entry> entries) {
        return new ReplyMap(entries);
    }

    static ReplyError error(ReplyErrorKind kind, String message) {
        return new ReplyError(kind, message);
    }
}
