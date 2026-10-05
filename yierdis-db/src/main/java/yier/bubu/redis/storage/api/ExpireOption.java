package yier.bubu.redis.storage.api;

// ExpireOption：SET 的过期选项（EX/PX/EXAT/PXAT/KEEPTTL），作为 command-facing 的稳定类型。

import java.util.Objects;

public final class ExpireOption {
    private enum Kind {
        KEEP_TTL,
        EX,
        PX,
        EXAT,
        PXAT
    }

    private final Kind kind;
    private final long value;

    private ExpireOption(Kind kind, long value) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.value = value;
    }

    public static ExpireOption keepTtl() {
        return new ExpireOption(Kind.KEEP_TTL, 0L);
    }

    public static ExpireOption ex(long seconds) {
        return new ExpireOption(Kind.EX, seconds);
    }

    public static ExpireOption px(long milliseconds) {
        return new ExpireOption(Kind.PX, milliseconds);
    }

    public static ExpireOption exAt(long unixSeconds) {
        return new ExpireOption(Kind.EXAT, unixSeconds);
    }

    public static ExpireOption pxAt(long unixMilliseconds) {
        return new ExpireOption(Kind.PXAT, unixMilliseconds);
    }

    public boolean isKeepTtl() {
        return kind == Kind.KEEP_TTL;
    }

    public long toExpireAtMillis(long nowMillis) {
        return switch (kind) {
            case KEEP_TTL -> throw new IllegalStateException("KEEP_TTL has no expireAtMillis");
            case EX -> relativeFromSeconds(nowMillis, value);
            case PX -> relativeFromMillis(nowMillis, value);
            case EXAT -> secondsToMillis(value);
            // PXAT 的参数已经是绝对毫秒。Long.MAX_VALUE 在 long 范围内，不是溢出。
            case PXAT -> value;
        };
    }

    // duration <= 0 保持“立即过期”：存储层直接用 px(0)/ex(0)，SET 命令则在解析期另报 invalid expire time。
    // 正数路径对齐 Redis getExpireMillisecondsOrReply：秒换算或 now+delta 放不下时报 'set'，不再钳到 Long.MAX_VALUE。
    private static long relativeFromSeconds(long nowMillis, long seconds) {
        if (seconds <= 0L) {
            return nowMillis;
        }
        return addBase(nowMillis, secondsToMillis(seconds));
    }

    private static long relativeFromMillis(long nowMillis, long milliseconds) {
        if (milliseconds <= 0L) {
            return nowMillis;
        }
        return addBase(nowMillis, milliseconds);
    }

    // nowMillis >= 0 时，Long.MAX_VALUE - nowMillis 不会回绕。命令路径传入的是当前墙钟，这里不拒绝负的 now。
    private static long addBase(long nowMillis, long deltaMillis) {
        if (deltaMillis > Long.MAX_VALUE - nowMillis) {
            throw invalidSetExpire();
        }
        return nowMillis + deltaMillis;
    }

    private static long secondsToMillis(long seconds) {
        if (seconds > Long.MAX_VALUE / 1000L || seconds < Long.MIN_VALUE / 1000L) {
            throw invalidSetExpire();
        }
        return seconds * 1000L;
    }

    private static YierdisCommandException invalidSetExpire() {
        return new YierdisCommandException("ERR invalid expire time in 'set' command");
    }
}
