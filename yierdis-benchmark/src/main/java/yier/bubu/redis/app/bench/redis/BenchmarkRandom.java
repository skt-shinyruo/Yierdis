package yier.bubu.redis.app.bench.redis;

import java.util.Objects;
import java.util.SplittableRandom;

public final class BenchmarkRandom {
    static final int TWELVE_DIGITS = 12;
    static final long TWELVE_DIGIT_LIMIT = 1_000_000_000_000L;

    private final SplittableRandom random;

    public BenchmarkRandom(long seed) {
        this.random = new SplittableRandom(seed);
    }

    public long nextLong(long bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be > 0");
        }
        return random.nextLong(bound);
    }

    static void writeTwelveDigits(byte[] target, int offset, long value) {
        Objects.requireNonNull(target, "target");
        if (offset < 0 || offset > target.length - TWELVE_DIGITS) {
            throw new IndexOutOfBoundsException("offset must leave room for 12 digits");
        }
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }
        if (value >= TWELVE_DIGIT_LIMIT) {
            throw new IllegalArgumentException("value must fit in 12 digits");
        }

        long remaining = value;
        for (int index = offset + TWELVE_DIGITS - 1; index >= offset; index--) {
            target[index] = (byte) ('0' + remaining % 10);
            remaining /= 10;
        }
    }
}
