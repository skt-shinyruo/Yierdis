package yier.bubu.redis.protocol.reply;

import java.util.List;

/**
 * Reply IR（协议语义中间层）的值模型。
 * <p>
 * 该模型用于表达“命令层回包语义”，避免将 JSON/RESP 的限制泄露到命令层假设里。
 * Custom Protocol v1 的 NDJSON encoder 负责将其映射到 wire 表示。
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
