package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeHandleDomain;
import yier.bubu.redis.memory.api.NativeObjectKind;

final class YierdisLocalHandleCodec {
    private static final int DOMAIN_SHIFT = 60;
    private static final int KIND_SHIFT = 56;
    private static final int SLOT_SHIFT = 16;
    private static final int GENERATION_SHIFT = 4;
    private static final long FOUR_BIT_MASK = 0x0fL;
    private static final long SLOT_MASK = (1L << 40) - 1L;
    private static final long GENERATION_MASK = (1L << 12) - 1L;

    private YierdisLocalHandleCodec() {
    }

    static long encode(
            NativeHandleDomain domain,
            NativeObjectKind kind,
            long slotId,
            int generation,
            int flags
    ) {
        Objects.requireNonNull(domain, "domain");
        Objects.requireNonNull(kind, "kind");
        if (domain == NativeHandleDomain.RESERVED || kind.domain() != domain) {
            throw new IllegalArgumentException("invalid local handle domain/kind");
        }
        if (slotId < 0L || slotId > SLOT_MASK) {
            throw new IllegalArgumentException("slotId out of range: " + slotId);
        }
        if (generation < 0 || generation > GENERATION_MASK) {
            throw new IllegalArgumentException("generation out of range: " + generation);
        }
        if (flags < 0 || flags > FOUR_BIT_MASK) {
            throw new IllegalArgumentException("flags out of range: " + flags);
        }
        return ((long) domain.code() << DOMAIN_SHIFT)
                | ((long) kind.code() << KIND_SHIFT)
                | (slotId << SLOT_SHIFT)
                | ((long) generation << GENERATION_SHIFT)
                | flags;
    }

    static void requireValid(long localRaw) {
        if (localRaw != 0L && domainCode(localRaw) == NativeHandleDomain.RESERVED.code()) {
            throw new IllegalArgumentException("non-zero local handle cannot use reserved domain");
        }
    }

    static NativeHandleDomain domain(long localRaw) {
        return NativeHandleDomain.fromCode(domainCode(localRaw));
    }

    static int kindCode(long localRaw) {
        return (int) ((localRaw >>> KIND_SHIFT) & FOUR_BIT_MASK);
    }

    static long slotId(long localRaw) {
        return (localRaw >>> SLOT_SHIFT) & SLOT_MASK;
    }

    static int generation(long localRaw) {
        return (int) ((localRaw >>> GENERATION_SHIFT) & GENERATION_MASK);
    }

    static int flags(long localRaw) {
        return (int) (localRaw & FOUR_BIT_MASK);
    }

    private static int domainCode(long localRaw) {
        return (int) ((localRaw >>> DOMAIN_SHIFT) & FOUR_BIT_MASK);
    }
}
