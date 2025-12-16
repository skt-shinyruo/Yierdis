package yier.bubu.redis.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class RespBulkString implements RespObject {
    private static final RespBulkString NULL = new RespBulkString(null);

    private final byte[] data;

    private RespBulkString(byte[] data) {
        this.data = data;
    }

    public static RespBulkString ofBytes(byte[] data) {
        return new RespBulkString(Arrays.copyOf(data, data.length));
    }

    public static RespBulkString ofString(String value) {
        if (value == null) {
            return NULL;
        }
        return new RespBulkString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static RespBulkString nullString() {
        return NULL;
    }

    public boolean isNull() {
        return data == null;
    }

    public byte[] data() {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }

    public String asString() {
        if (data == null) {
            return null;
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public RespType type() {
        return RespType.BULK_STRING;
    }

    @Override
    public String toHumanReadableString() {
        String s = asString();
        return s == null ? "(nil)" : s;
    }
}
