package yier.bubu.redis.ops;

/**
 * Generic command-facing write result.
 */
public final class WriteResult<T> {
    private final T value;
    private final MutationOutcome mutationOutcome;

    private WriteResult(T value, MutationOutcome mutationOutcome) {
        this.value = value;
        this.mutationOutcome = mutationOutcome == null ? MutationOutcome.NONE : mutationOutcome;
    }

    public static <T> WriteResult<T> of(T value, MutationOutcome mutationOutcome) {
        return new WriteResult<>(value, mutationOutcome);
    }

    public static <T> WriteResult<T> unchanged(T value) {
        return new WriteResult<>(value, MutationOutcome.NONE);
    }

    public T value() {
        return value;
    }

    public MutationOutcome mutationOutcome() {
        return mutationOutcome;
    }

    public boolean changedAny() {
        return mutationOutcome.changedAny();
    }
}
