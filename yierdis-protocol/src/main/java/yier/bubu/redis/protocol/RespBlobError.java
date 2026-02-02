package yier.bubu.redis.protocol;

// RESP3 blob error（!len\r\n...）对象：用于 CLI/测试解析与类型断言。

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class RespBlobError implements RespObject {
    private final byte[] data;

    private RespBlobError(byte[] data) {
        this.data = data == null ? null : Arrays.copyOf(data, data.length);
    }

    public static RespBlobError ofBytes(byte[] data) {
        return new RespBlobError(data);
    }

    public byte[] data() {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }

    public String asString() {
        return data == null ? null : new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public RespType type() {
        return RespType.BLOB_ERROR;
    }

    @Override
    public String toHumanReadableString() {
        String s = asString();
        return s == null ? "(null)" : s;
    }
}

