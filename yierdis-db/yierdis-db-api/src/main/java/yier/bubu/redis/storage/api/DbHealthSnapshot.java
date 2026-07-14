package yier.bubu.redis.storage.api;

public record DbHealthSnapshot(
        boolean degraded,
        String failureTypeName,
        String failureMessage,
        long failureAtMillis
) {
    private static final DbHealthSnapshot HEALTHY = new DbHealthSnapshot(false, null, null, 0L);

    public static DbHealthSnapshot healthy() {
        return HEALTHY;
    }

    public static DbHealthSnapshot degraded(Throwable failure, long failureAtMillis) {
        if (failure == null) {
            throw new IllegalArgumentException("failure must not be null");
        }
        if (failureAtMillis <= 0L) {
            throw new IllegalArgumentException("failureAtMillis must be > 0");
        }
        return new DbHealthSnapshot(
                true,
                failure.getClass().getName(),
                failure.getMessage(),
                failureAtMillis
        );
    }
}
