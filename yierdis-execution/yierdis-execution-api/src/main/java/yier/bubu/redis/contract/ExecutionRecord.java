package yier.bubu.redis.contract;

import java.util.Objects;

/**
 * Immutable replay record for execution-layer requests.
 */
public record ExecutionRecord(int dbIndex, ExecutionRequest request) {
    public ExecutionRecord {
        request = ByteArrayExecutionRequest.copyOf(Objects.requireNonNull(request, "request"));
        dbIndex = Math.max(0, dbIndex);
    }
}
