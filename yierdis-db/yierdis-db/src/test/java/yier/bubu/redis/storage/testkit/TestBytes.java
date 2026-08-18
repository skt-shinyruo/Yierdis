package yier.bubu.redis.storage.testkit;

import java.nio.charset.StandardCharsets;

public final class TestBytes {
    private TestBytes() {
    }

    public static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
