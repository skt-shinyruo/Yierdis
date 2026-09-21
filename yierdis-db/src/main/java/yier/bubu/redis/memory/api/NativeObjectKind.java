package yier.bubu.redis.memory.api;

public enum NativeObjectKind {
    GENERIC(0, NativeHandleDomain.STORAGE_OBJECT),
    STRING_BYTES(1, NativeHandleDomain.STORAGE_OBJECT),
    LISTPACK_BYTES(2, NativeHandleDomain.STORAGE_OBJECT),
    HASH_FIELD_BYTES(3, NativeHandleDomain.STORAGE_OBJECT),
    HASH_VALUE_BYTES(4, NativeHandleDomain.STORAGE_OBJECT),
    SET_MEMBER_BYTES(5, NativeHandleDomain.STORAGE_OBJECT),
    ZSET_MEMBER_BYTES(6, NativeHandleDomain.STORAGE_OBJECT),
    SCORE_BYTES(7, NativeHandleDomain.STORAGE_OBJECT),

    ENTRY_RECORD(1, NativeHandleDomain.ENTRY_OBJECT),
    KEY_BYTES(1, NativeHandleDomain.KEY_BYTES),

    LIST_ROOT(1, NativeHandleDomain.TYPE_ROOT),
    HASH_ROOT(2, NativeHandleDomain.TYPE_ROOT),
    SET_ROOT(3, NativeHandleDomain.TYPE_ROOT),
    ZSET_ROOT(4, NativeHandleDomain.TYPE_ROOT),
    LIST_NODE(5, NativeHandleDomain.TYPE_ROOT),
    HASH_TABLE(6, NativeHandleDomain.TYPE_ROOT),
    SET_TABLE(7, NativeHandleDomain.TYPE_ROOT),
    ZSET_TABLE(8, NativeHandleDomain.TYPE_ROOT),
    ZSET_NODE(9, NativeHandleDomain.TYPE_ROOT),

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
