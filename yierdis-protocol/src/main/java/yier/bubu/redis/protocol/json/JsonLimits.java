package yier.bubu.redis.protocol.json;

import java.util.Objects;

/**
 * Limits for JSON parsing to reduce DoS/memory risks.
 */
public final class JsonLimits {
    public static final JsonLimits DEFAULT = new JsonLimits(64, 1024, 1024, 64 * 1024 * 1024);

    private final int maxNestingDepth;
    private final int maxArrayLen;
    private final int maxObjectPairs;
    private final int maxStringChars;

    public JsonLimits(int maxNestingDepth, int maxArrayLen, int maxObjectPairs, int maxStringChars) {
        if (maxNestingDepth <= 0) {
            throw new IllegalArgumentException("maxNestingDepth must be > 0");
        }
        if (maxArrayLen < 0) {
            throw new IllegalArgumentException("maxArrayLen must be >= 0");
        }
        if (maxObjectPairs < 0) {
            throw new IllegalArgumentException("maxObjectPairs must be >= 0");
        }
        if (maxStringChars < 0) {
            throw new IllegalArgumentException("maxStringChars must be >= 0");
        }
        this.maxNestingDepth = maxNestingDepth;
        this.maxArrayLen = maxArrayLen;
        this.maxObjectPairs = maxObjectPairs;
        this.maxStringChars = maxStringChars;
    }

    public int maxNestingDepth() {
        return maxNestingDepth;
    }

    public int maxArrayLen() {
        return maxArrayLen;
    }

    public int maxObjectPairs() {
        return maxObjectPairs;
    }

    public int maxStringChars() {
        return maxStringChars;
    }

    public static JsonLimits orDefault(JsonLimits limits) {
        return Objects.requireNonNullElse(limits, DEFAULT);
    }
}

