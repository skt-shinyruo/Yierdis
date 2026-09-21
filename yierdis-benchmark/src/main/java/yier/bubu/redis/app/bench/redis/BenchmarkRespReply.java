package yier.bubu.redis.app.bench.redis;

import java.util.Objects;

public final class BenchmarkRespReply {
    public enum Kind {
        SIMPLE_STRING,
        ERROR,
        INTEGER,
        BULK_STRING,
        NULL_BULK,
        ARRAY,
        NULL_ARRAY
    }

    private static final BenchmarkRespReply INTEGER = new BenchmarkRespReply(Kind.INTEGER, null);
    private static final BenchmarkRespReply BULK = new BenchmarkRespReply(Kind.BULK_STRING, null);
    private static final BenchmarkRespReply NULL_BULK = new BenchmarkRespReply(Kind.NULL_BULK, null);
    private static final BenchmarkRespReply ARRAY = new BenchmarkRespReply(Kind.ARRAY, null);
    private static final BenchmarkRespReply NULL_ARRAY = new BenchmarkRespReply(Kind.NULL_ARRAY, null);

    private final Kind kind;
    private final String text;

    private BenchmarkRespReply(Kind kind, String text) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.text = text;
    }

    static BenchmarkRespReply simpleString(String text) {
        return new BenchmarkRespReply(Kind.SIMPLE_STRING, Objects.requireNonNull(text, "text"));
    }

    static BenchmarkRespReply error(String text) {
        return new BenchmarkRespReply(Kind.ERROR, Objects.requireNonNull(text, "text"));
    }

    static BenchmarkRespReply integer() {
        return INTEGER;
    }

    static BenchmarkRespReply bulkString() {
        return BULK;
    }

    static BenchmarkRespReply nullBulk() {
        return NULL_BULK;
    }

    static BenchmarkRespReply array() {
        return ARRAY;
    }

    static BenchmarkRespReply nullArray() {
        return NULL_ARRAY;
    }

    public Kind kind() {
        return kind;
    }

    public String text() {
        return text;
    }
}
