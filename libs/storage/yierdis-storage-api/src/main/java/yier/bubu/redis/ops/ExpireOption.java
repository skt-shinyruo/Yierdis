package yier.bubu.redis.ops;

// ExpireOption：SET 的过期选项（EX/PX/EXAT/PXAT/KEEPTTL），作为 command-facing 的稳定类型。

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class ExpireOption {
    public enum Kind {
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
            case EX -> safeExpireRelativeMillis(nowMillis, value, TimeUnit.SECONDS);
            case PX -> safeExpireRelativeMillis(nowMillis, value, TimeUnit.MILLISECONDS);
            case EXAT -> safeExpireAbsoluteMillis(value, TimeUnit.SECONDS);
            case PXAT -> safeExpireAbsoluteMillis(value, TimeUnit.MILLISECONDS);
        };
    }

    private static long safeExpireRelativeMillis(long nowMillis, long duration, TimeUnit unit) {
        if (duration <= 0) {
            return nowMillis;
        }
        long deltaMillis;
        try {
            deltaMillis = Math.multiplyExact(duration, unit == TimeUnit.SECONDS ? 1000L : 1L);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long safeExpireAbsoluteMillis(long value, TimeUnit unit) {
        if (unit == TimeUnit.MILLISECONDS) {
            return value;
        }
        try {
            return Math.multiplyExact(value, 1000L);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}

