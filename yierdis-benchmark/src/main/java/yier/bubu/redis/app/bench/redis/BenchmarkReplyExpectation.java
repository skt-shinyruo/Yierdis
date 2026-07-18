package yier.bubu.redis.app.bench.redis;

public enum BenchmarkReplyExpectation {
    PONG,
    OK,
    INTEGER,
    BULK_OR_NULL,
    ARRAY
}
