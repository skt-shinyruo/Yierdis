package yier.bubu.redis.storage.memory.internal.entry;

public final class NativeStorageLayout {
    public static final int ENTRY_RECORD_BYTES = 72;
    public static final int COLLECTION_ROOT_RECORD_BYTES = Long.BYTES * 2;

    private NativeStorageLayout() {
    }
}
