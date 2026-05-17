package yier.bubu.redis.memory.api;

import java.util.Objects;

public record NativeObjectKindCounts(
        long genericObjects,
        long stringByteObjects,
        long entryRecordObjects,
        long keyByteObjects,
        long listNodeObjects,
        long listQuicklistNodeObjects,
        long hashNodeObjects,
        long setNodeObjects,
        long zsetNodeObjects,
        long indexNodeObjects,
        long metadataRecordObjects
) {
    public NativeObjectKindCounts {
        requireNonNegative(genericObjects, "genericObjects");
        requireNonNegative(stringByteObjects, "stringByteObjects");
        requireNonNegative(entryRecordObjects, "entryRecordObjects");
        requireNonNegative(keyByteObjects, "keyByteObjects");
        requireNonNegative(listNodeObjects, "listNodeObjects");
        requireNonNegative(listQuicklistNodeObjects, "listQuicklistNodeObjects");
        requireNonNegative(hashNodeObjects, "hashNodeObjects");
        requireNonNegative(setNodeObjects, "setNodeObjects");
        requireNonNegative(zsetNodeObjects, "zsetNodeObjects");
        requireNonNegative(indexNodeObjects, "indexNodeObjects");
        requireNonNegative(metadataRecordObjects, "metadataRecordObjects");
    }

    public static NativeObjectKindCounts empty() {
        return new NativeObjectKindCounts(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public long count(NativeObjectKind kind) {
        Objects.requireNonNull(kind, "kind");
        return switch (kind) {
            case GENERIC -> genericObjects;
            case STRING_BYTES -> stringByteObjects;
            case ENTRY_RECORD -> entryRecordObjects;
            case KEY_BYTES -> keyByteObjects;
            case LIST_NODE -> listNodeObjects;
            case LIST_QUICKLIST_NODE -> listQuicklistNodeObjects;
            case HASH_NODE -> hashNodeObjects;
            case SET_NODE -> setNodeObjects;
            case ZSET_NODE -> zsetNodeObjects;
            case INDEX_NODE -> indexNodeObjects;
            case METADATA_RECORD -> metadataRecordObjects;
        };
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
