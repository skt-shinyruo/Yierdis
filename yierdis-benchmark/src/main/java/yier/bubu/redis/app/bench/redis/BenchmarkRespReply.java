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

    private static final BenchmarkRespReply NULL_BULK =
            new BenchmarkRespReply(Kind.NULL_BULK, null, 0, -1, -1);
    private static final BenchmarkRespReply NULL_ARRAY =
            new BenchmarkRespReply(Kind.NULL_ARRAY, null, 0, -1, -1);

    private final Kind kind;
    private final String text;
    private final long integer;
    private final int bulkLength;
    private final int arrayLength;

    private BenchmarkRespReply(Kind kind, String text, long integer, int bulkLength, int arrayLength) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.text = text;
        this.integer = integer;
        this.bulkLength = bulkLength;
        this.arrayLength = arrayLength;
    }

    static BenchmarkRespReply simpleString(String text) {
        return new BenchmarkRespReply(
                Kind.SIMPLE_STRING, Objects.requireNonNull(text, "text"), 0, -1, -1
        );
    }

    static BenchmarkRespReply error(String text) {
        return new BenchmarkRespReply(
                Kind.ERROR, Objects.requireNonNull(text, "text"), 0, -1, -1
        );
    }

    static BenchmarkRespReply integer(long value) {
        return new BenchmarkRespReply(Kind.INTEGER, null, value, -1, -1);
    }

    static BenchmarkRespReply bulkString(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        return new BenchmarkRespReply(Kind.BULK_STRING, null, 0, length, -1);
    }

    static BenchmarkRespReply nullBulk() {
        return NULL_BULK;
    }

    static BenchmarkRespReply array(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        return new BenchmarkRespReply(Kind.ARRAY, null, 0, -1, length);
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

    public Long integer() {
        return kind == Kind.INTEGER ? Long.valueOf(integer) : null;
    }

    public long integerValue() {
        if (kind != Kind.INTEGER) {
            throw new IllegalStateException("reply is not an integer");
        }
        return integer;
    }

    public int bulkLength() {
        return bulkLength;
    }

    public int arrayLength() {
        return arrayLength;
    }
}
