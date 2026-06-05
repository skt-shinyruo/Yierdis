package yier.bubu.redis.memory.api;

import java.util.Objects;

public record NativeObjectKindCounts(
        long genericObjects,
        long stringByteObjects,
        long listpackByteObjects,
        long hashFieldByteObjects,
        long hashValueByteObjects,
        long setMemberByteObjects,
        long zsetMemberByteObjects,
        long scoreByteObjects,
        long entryRecordObjects,
        long keyByteObjects,
        long listRootObjects,
        long hashRootObjects,
        long setRootObjects,
        long zsetRootObjects,
        long listNodeObjects,
        long hashTableObjects,
        long setTableObjects,
        long zsetTableObjects,
        long zsetNodeObjects,
        long indexNodeObjects,
        long metadataRecordObjects
) {
    public NativeObjectKindCounts {
        requireNonNegative(genericObjects, "genericObjects");
        requireNonNegative(stringByteObjects, "stringByteObjects");
        requireNonNegative(listpackByteObjects, "listpackByteObjects");
        requireNonNegative(hashFieldByteObjects, "hashFieldByteObjects");
        requireNonNegative(hashValueByteObjects, "hashValueByteObjects");
        requireNonNegative(setMemberByteObjects, "setMemberByteObjects");
        requireNonNegative(zsetMemberByteObjects, "zsetMemberByteObjects");
        requireNonNegative(scoreByteObjects, "scoreByteObjects");
        requireNonNegative(entryRecordObjects, "entryRecordObjects");
        requireNonNegative(keyByteObjects, "keyByteObjects");
        requireNonNegative(listRootObjects, "listRootObjects");
        requireNonNegative(hashRootObjects, "hashRootObjects");
        requireNonNegative(setRootObjects, "setRootObjects");
        requireNonNegative(zsetRootObjects, "zsetRootObjects");
        requireNonNegative(listNodeObjects, "listNodeObjects");
        requireNonNegative(hashTableObjects, "hashTableObjects");
        requireNonNegative(setTableObjects, "setTableObjects");
        requireNonNegative(zsetTableObjects, "zsetTableObjects");
        requireNonNegative(zsetNodeObjects, "zsetNodeObjects");
        requireNonNegative(indexNodeObjects, "indexNodeObjects");
        requireNonNegative(metadataRecordObjects, "metadataRecordObjects");
    }

    public static NativeObjectKindCounts empty() {
        return new NativeObjectKindCounts(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public long count(NativeObjectKind kind) {
        Objects.requireNonNull(kind, "kind");
        return switch (kind) {
            case GENERIC -> genericObjects;
            case STRING_BYTES -> stringByteObjects;
            case LISTPACK_BYTES -> listpackByteObjects;
            case HASH_FIELD_BYTES -> hashFieldByteObjects;
            case HASH_VALUE_BYTES -> hashValueByteObjects;
            case SET_MEMBER_BYTES -> setMemberByteObjects;
            case ZSET_MEMBER_BYTES -> zsetMemberByteObjects;
            case SCORE_BYTES -> scoreByteObjects;
            case ENTRY_RECORD -> entryRecordObjects;
            case KEY_BYTES -> keyByteObjects;
            case LIST_ROOT -> listRootObjects;
            case HASH_ROOT -> hashRootObjects;
            case SET_ROOT -> setRootObjects;
            case ZSET_ROOT -> zsetRootObjects;
            case LIST_NODE -> listNodeObjects;
            case HASH_TABLE -> hashTableObjects;
            case SET_TABLE -> setTableObjects;
            case ZSET_TABLE -> zsetTableObjects;
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
