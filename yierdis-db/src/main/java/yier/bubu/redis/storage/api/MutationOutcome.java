package yier.bubu.redis.storage.api;

/**
 * Explicit facts about durable state changed by a storage write.
 */
public enum MutationOutcome {
    NONE(false, false),
    VALUE_CHANGED(true, false),
    TTL_CHANGED(false, true),
    VALUE_AND_TTL_CHANGED(true, true);

    private final boolean valueChanged;
    private final boolean ttlChanged;

    MutationOutcome(boolean valueChanged, boolean ttlChanged) {
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
