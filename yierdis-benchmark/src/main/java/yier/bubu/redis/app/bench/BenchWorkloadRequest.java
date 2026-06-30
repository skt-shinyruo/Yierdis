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
        boolean strictReplies,
        boolean externalRedis,
        String redisUser,
        String redisAuth,
        int redisDb
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
        if (redisDb < 0) {
            throw new IllegalArgumentException("redisDb must be >= 0");
        }
        if (!externalRedis && (!(redisUser == null || redisUser.isBlank())
                || !(redisAuth == null || redisAuth.isBlank())
                || redisDb != 0)) {
            throw new IllegalArgumentException("redis auth/db options require externalRedis");
        }
        redisUser = redisUser == null ? "" : redisUser;
        redisAuth = redisAuth == null ? "" : redisAuth;
    }

    public BenchWorkloadRequest(
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
        this(workload, host, port, requests, clients, pipeline, keyspace, dataSize, latency, strictReplies, false, "", "", 0);
    }

    public BenchWorkloadRequest(
            BenchWorkloadKind workload,
            String host,
            int port,
            int requests,
            int clients,
            int pipeline,
            int keyspace,
            int dataSize,
            boolean latency,
            boolean strictReplies,
            String redisUser,
            String redisAuth,
            int redisDb
    ) {
        this(workload, host, port, requests, clients, pipeline, keyspace, dataSize, latency, strictReplies, true,
                redisUser, redisAuth, redisDb);
    }
}
