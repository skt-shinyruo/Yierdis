package yier.bubu.redis.memory.api;

public enum NativeObjectKind {
    GENERIC(0, NativeHandleDomain.STORAGE_OBJECT),
    STRING_BYTES(1, NativeHandleDomain.STORAGE_OBJECT),
    ENTRY_RECORD(1, NativeHandleDomain.ENTRY_OBJECT),
    KEY_BYTES(1, NativeHandleDomain.KEY_BYTES),
    LIST_NODE(2, NativeHandleDomain.TYPE_ROOT),
    HASH_NODE(3, NativeHandleDomain.TYPE_ROOT),
    SET_NODE(4, NativeHandleDomain.TYPE_ROOT),
    ZSET_NODE(5, NativeHandleDomain.TYPE_ROOT),
    INDEX_NODE(1, NativeHandleDomain.INDEX_NODE),
    METADATA_RECORD(1, NativeHandleDomain.ALLOCATOR_METADATA);

    private final int code;
    private final NativeHandleDomain domain;

    NativeObjectKind(int code, NativeHandleDomain domain) {
        this.code = code;
        this.domain = domain;
    }

    public int code() {
        return code;
    }

    public NativeHandleDomain domain() {
        return domain;
    }
}
