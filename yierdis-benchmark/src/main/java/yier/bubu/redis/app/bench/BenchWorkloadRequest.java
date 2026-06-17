package yier.bubu.redis.app.bench;

public record BenchWorkloadRequest(
        BenchWorkloadKind workload,
        String host,
        int port,
        int requests,
        int clients,
        int pipeline,
        int keyspace,
        int dataSize,
        boolean latency,
        boolean strictReplies
) {
    public BenchWorkloadRequest {
        if (workload == null) {
            throw new IllegalArgumentException("workload must not be null");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in range 1..65535");
        }
        if (requests <= 0) {
            throw new IllegalArgumentException("requests must be > 0");
        }
        if (clients <= 0) {
            throw new IllegalArgumentException("clients must be > 0");
        }
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        if (keyspace <= 0) {
            throw new IllegalArgumentException("keyspace must be > 0");
        }
        if (dataSize < 0) {
            throw new IllegalArgumentException("dataSize must be >= 0");
        }
    }
}
