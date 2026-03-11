package yier.bubu.redis.db.memory.api;

import java.util.Locale;

public enum YierdisOffHeapBackend {
    NONE,
    NETTY,
    UNSAFE,
    FOREIGN;

    public static YierdisOffHeapBackend fromString(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        String v = value.trim().toUpperCase(Locale.ROOT);
        if ("NONE".equals(v) || "OFF".equals(v) || "DISABLED".equals(v)) {
            return NONE;
        }
        if ("NETTY".equals(v)) {
            return NETTY;
        }
        if ("UNSAFE".equals(v)) {
            return UNSAFE;
        }
        if ("FOREIGN".equals(v) || "FMA".equals(v)) {
            return FOREIGN;
        }
        throw new IllegalArgumentException("unknown offheap backend: " + value);
    }
}
