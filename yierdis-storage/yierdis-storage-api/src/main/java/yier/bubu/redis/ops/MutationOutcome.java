package yier.bubu.redis.ops;

/**
 * Explicit facts about durable state changed by a storage write.
 */
public final class MutationOutcome {
    public static final MutationOutcome NONE = new MutationOutcome(false, false);
    public static final MutationOutcome VALUE_CHANGED = new MutationOutcome(true, false);
    public static final MutationOutcome TTL_CHANGED = new MutationOutcome(false, true);
    public static final MutationOutcome VALUE_AND_TTL_CHANGED = new MutationOutcome(true, true);

    private final boolean valueChanged;
    private final boolean ttlChanged;

    private MutationOutcome(boolean valueChanged, boolean ttlChanged) {
        this.valueChanged = valueChanged;
        this.ttlChanged = ttlChanged;
    }

    public static MutationOutcome of(boolean valueChanged, boolean ttlChanged) {
        if (valueChanged && ttlChanged) {
            return VALUE_AND_TTL_CHANGED;
        }
        if (valueChanged) {
            return VALUE_CHANGED;
        }
        if (ttlChanged) {
            return TTL_CHANGED;
        }
        return NONE;
    }

    public boolean valueChanged() {
        return valueChanged;
    }

    public boolean ttlChanged() {
        return ttlChanged;
    }

    public boolean changedAny() {
        return valueChanged || ttlChanged;
    }

    public MutationOutcome plus(MutationOutcome other) {
        if (other == null || other == NONE) {
            return this;
        }
        return of(valueChanged || other.valueChanged, ttlChanged || other.ttlChanged);
    }
}
