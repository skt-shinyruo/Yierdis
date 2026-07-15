package yier.bubu.redis.memory.api;

public enum NativeHandleDomain {
    RESERVED(0),
    STORAGE_OBJECT(1),
    ENTRY_OBJECT(2),
    KEY_BYTES(3),
    TYPE_ROOT(4),
    INDEX_NODE(5),
    ALLOCATOR_METADATA(6);

    private static final NativeHandleDomain[] BY_CODE = lookupByCode();

    private final int code;

    NativeHandleDomain(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static NativeHandleDomain fromCode(int code) {
        if (code >= 0 && code < BY_CODE.length) {
            NativeHandleDomain domain = BY_CODE[code];
            if (domain != null) {
                return domain;
            }
        }
        throw new IllegalArgumentException("unknown native handle domain: " + code);
    }

    private static NativeHandleDomain[] lookupByCode() {
        NativeHandleDomain[] domains = values();
        int highestCode = -1;
        for (NativeHandleDomain domain : domains) {
            highestCode = Math.max(highestCode, domain.code);
        }
        NativeHandleDomain[] byCode = new NativeHandleDomain[highestCode + 1];
        for (NativeHandleDomain domain : domains) {
            if (byCode[domain.code] != null) {
                throw new IllegalStateException("duplicate native handle domain code: " + domain.code);
            }
            byCode[domain.code] = domain;
        }
        return byCode;
    }
}
