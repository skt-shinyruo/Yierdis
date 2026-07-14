package yier.bubu.redis.runtime.embedded;

/**
 * CommitStream 的瞬时观测快照。
 */
public record CommitStreamStats(
        CommitStreamState state,
        long reservedEvents,
        long reservedBytes,
        long rejectedWrites,
        long lastAssignedSequence,
        long lastAcknowledgedSequence,
        String firstFailureType,
        String firstFailureMessage,
        boolean callbackActive,
        boolean shutdownTimedOut
) {
    public static CommitStreamStats disabled() {
        return new CommitStreamStats(
                CommitStreamState.DISABLED,
                0L,
                0L,
                0L,
                0L,
                0L,
                null,
                null,
                false,
                false
        );
    }
}
