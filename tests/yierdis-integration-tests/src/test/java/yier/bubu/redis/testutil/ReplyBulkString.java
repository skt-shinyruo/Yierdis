package yier.bubu.redis.testutil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class ReplyBulkString implements ReplyObject {
    private final byte[] data;

    public ReplyBulkString(byte[] data) {
        Objects.requireNonNull(data, "data");
        this.data = Arrays.copyOf(data, data.length);
    }

    public byte[] data() {
        return Arrays.copyOf(data, data.length);
    }

    public String asString() {
        return new String(data, StandardCharsets.UTF_8);
    }
}

