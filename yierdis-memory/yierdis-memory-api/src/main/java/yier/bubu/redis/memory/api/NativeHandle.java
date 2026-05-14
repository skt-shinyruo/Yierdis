package yier.bubu.redis.memory.api;

import java.util.Objects;

public record NativeHandle(long raw) {
    public static final NativeHandle NULL = new NativeHandle(0L);

    private static final int DOMAIN_SHIFT = 60;
    private static final int KIND_SHIFT = 56;
    private static final int SLOT_SHIFT = 16;
    private static final int GENERATION_SHIFT = 4;

    private static final long FOUR_BIT_MASK = 0x0fL;
    private static final long SLOT_MASK = (1L << 40) - 1L;
    private static final long GENERATION_MASK = (1L << 12) - 1L;

    public NativeHandle {
        if (raw != 0L && ((raw >>> DOMAIN_SHIFT) & FOUR_BIT_MASK) == NativeHandleDomain.RESERVED.code()) {
            throw new IllegalArgumentException("non-zero handle cannot use reserved domain");
        }
    }

    public static NativeHandle fromRaw(long raw) {
        return new NativeHandle(raw);
    }

    public static NativeHandle of(
            NativeHandleDomain domain,
            NativeObjectKind kind,
            long slotId,
            int generation,
            int flags
    ) {
        Objects.requireNonNull(domain, "domain");
        Objects.requireNonNull(kind, "kind");
        if (domain == NativeHandleDomain.RESERVED) {
            throw new IllegalArgumentException("domain must not be reserved");
        }
        if (slotId < 0 || slotId > SLOT_MASK) {
            throw new IllegalArgumentException("slotId out of range: " + slotId);
        }
        if (generation < 0 || generation > GENERATION_MASK) {
            throw new IllegalArgumentException("generation out of range: " + generation);
        }
        if (flags < 0 || flags > FOUR_BIT_MASK) {
            throw new IllegalArgumentException("flags out of range: " + flags);
        }
        if (kind.code() < 0 || kind.code() > FOUR_BIT_MASK) {
            throw new IllegalArgumentException("kind code out of range: " + kind.code());
        }
        long raw = ((long) domain.code() << DOMAIN_SHIFT)
                | ((long) kind.code() << KIND_SHIFT)
                | (slotId << SLOT_SHIFT)
                | ((long) generation << GENERATION_SHIFT)
                | flags;
        return new NativeHandle(raw);
    }

    public boolean isNull() {
        return raw == 0L;
    }

    public NativeHandleDomain domain() {
        return NativeHandleDomain.fromCode((int) ((raw >>> DOMAIN_SHIFT) & FOUR_BIT_MASK));
    }

    public int kindCode() {
        return (int) ((raw >>> KIND_SHIFT) & FOUR_BIT_MASK);
    }

    public long slotId() {
        return (raw >>> SLOT_SHIFT) & SLOT_MASK;
    }

    public int generation() {
        return (int) ((raw >>> GENERATION_SHIFT) & GENERATION_MASK);
    }

    public int flags() {
        return (int) (raw & FOUR_BIT_MASK);
    }

    public void requireNonNull() {
        if (isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }
    }
}
