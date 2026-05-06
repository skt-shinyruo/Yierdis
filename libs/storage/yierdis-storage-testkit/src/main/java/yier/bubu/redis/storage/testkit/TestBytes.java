package yier.bubu.redis.storage.testkit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class TestBytes {
    private TestBytes() {
    }

    public static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static List<byte[]> cmd(String... parts) {
        List<byte[]> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            out.add(b(p));
        }
        return out;
    }
}
