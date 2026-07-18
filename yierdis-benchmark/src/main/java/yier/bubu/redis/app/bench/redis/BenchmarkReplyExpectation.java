package yier.bubu.redis.app.bench.redis;

public enum BenchmarkReplyExpectation {
    PONG,
    OK,
    INTEGER,
    BULK_OR_NULL,
    ARRAY;

    String failureDetail(BenchmarkRespReply reply) {
        BenchmarkRespReply requiredReply = java.util.Objects.requireNonNull(reply, "reply");
        if (requiredReply.kind() == BenchmarkRespReply.Kind.ERROR) {
            return requiredReply.text();
        }
        boolean matches = switch (this) {
            case PONG -> requiredReply.kind() == BenchmarkRespReply.Kind.SIMPLE_STRING
                    && "PONG".equals(requiredReply.text());
            case OK -> requiredReply.kind() == BenchmarkRespReply.Kind.SIMPLE_STRING
                    && "OK".equals(requiredReply.text());
            case INTEGER -> requiredReply.kind() == BenchmarkRespReply.Kind.INTEGER;
            case BULK_OR_NULL -> requiredReply.kind() == BenchmarkRespReply.Kind.BULK_STRING
                    || requiredReply.kind() == BenchmarkRespReply.Kind.NULL_BULK;
            case ARRAY -> requiredReply.kind() == BenchmarkRespReply.Kind.ARRAY;
        };
        if (matches) {
            return null;
        }
        return switch (this) {
            case PONG -> "expected PONG";
            case OK -> "expected OK";
            case INTEGER -> "expected an integer reply";
            case BULK_OR_NULL -> "expected a bulk or null bulk reply";
            case ARRAY -> "expected an array reply";
        };
    }
}
