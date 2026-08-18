package yier.bubu.redis.storage.api;

/**
 * Generic command-facing write result.
 */
public record WriteResult<T>(T value, MutationOutcome mutationOutcome) {
    public WriteResult {
        mutationOutcome = mutationOutcome == null ? MutationOutcome.NONE : mutationOutcome;
    }

    public static <T> WriteResult<T> of(T value, MutationOutcome mutationOutcome) {
        return new WriteResult<>(value, mutationOutcome);
    }

    public static <T> WriteResult<T> unchanged(T value) {
        return new WriteResult<>(value, MutationOutcome.NONE);
    }

}
