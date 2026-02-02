package yier.bubu.redis.protocol;

// RESP3 verbatim string（=len\r\nfmt:payload\r\n）对象：用于 CLI/测试解析与类型断言。

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class RespVerbatimString implements RespObject {
    private final String format;
    private final byte[] data;

    private RespVerbatimString(String format, byte[] data) {
        this.format = Objects.requireNonNull(format, "format");
        this.data = data == null ? null : Arrays.copyOf(data, data.length);
    }

    public static RespVerbatimString ofBytes(String format, byte[] data) {
        if (format == null || format.length() != 3) {
            throw new IllegalArgumentException("format must be 3 chars");
        }
        return new RespVerbatimString(format, data);
    }

    public String format() {
        return format;
    }

    public byte[] data() {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }

    public String asString() {
        return data == null ? null : new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public RespType type() {
        return RespType.VERBATIM_STRING;
    }

    @Override
    public String toHumanReadableString() {
        return format + ":" + (data == null ? "(null)" : asString());
    }
}

