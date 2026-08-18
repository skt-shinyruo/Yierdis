package yier.bubu.redis.testutil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public record ReplyBulkString(byte[] data) implements ReplyObject {
    public ReplyBulkString(byte[] data) {
        this.data = Arrays.copyOf(Objects.requireNonNull(data, "data"), data.length);
    }

    @Override
    public byte[] data() {
        return Arrays.copyOf(data, data.length);
    }

    public String asString() {
        return new String(data, StandardCharsets.UTF_8);
    }
}
