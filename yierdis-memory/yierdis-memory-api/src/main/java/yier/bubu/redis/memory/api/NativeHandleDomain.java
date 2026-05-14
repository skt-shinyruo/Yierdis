package yier.bubu.redis.memory.api;

public enum NativeHandleDomain {
    RESERVED(0),
    STORAGE_OBJECT(1),
    ENTRY_OBJECT(2),
    KEY_BYTES(3),
    TYPE_ROOT(4),
    INDEX_NODE(5),
    ALLOCATOR_METADATA(6);

    private final int code;

    NativeHandleDomain(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static NativeHandleDomain fromCode(int code) {
        for (NativeHandleDomain domain : values()) {
            if (domain.code == code) {
                return domain;
            }
        }
        throw new IllegalArgumentException("unknown native handle domain: " + code);
    }
}
