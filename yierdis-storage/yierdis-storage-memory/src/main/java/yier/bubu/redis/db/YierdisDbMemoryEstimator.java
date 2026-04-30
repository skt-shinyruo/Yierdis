package yier.bubu.redis.db;

import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.ValueType;

import java.util.List;

final class YierdisDbMemoryEstimator {
    private static final long SET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final long ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 96L;

    private final boolean keysStoredOffHeap;
    private final OffHeapAllocator offHeapAllocator;

    YierdisDbMemoryEstimator(boolean keysStoredOffHeap, OffHeapAllocator offHeapAllocator) {
        this.keysStoredOffHeap = keysStoredOffHeap;
        this.offHeapAllocator = offHeapAllocator;
    }

    long estimateEntryBytes(KeyHandle keyHandle, YierdisObject object) {
        if (keyHandle == null || object == null) {
            return 0;
        }
        int keyLen = Math.max(0, keyHandle.len());
        int keyBytesCost = keysStoredOffHeap ? 0 : keyLen;
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + keyBytesCost + estimateValueBytes(object);
    }

    private long estimateValueBytes(YierdisObject object) {
        if (object == null) {
            return 0;
        }
        if (object.type == ValueType.STRING) {
            if (object.encoding == ValueEncoding.STRING_INT) {
                return Long.BYTES;
            }
            if (offHeapAllocator != null && object.payload instanceof OffHeapBuf) {
                return 0;
            }
            return object.rawLen;
        }

        if (object.payload instanceof HashValue hv) {
            return hv.estimatedBytes();
        }
        if (object.payload instanceof ListValue lv) {
            return lv.estimatedBytes();
        }
        if (object.payload instanceof SetValue sv) {
            return sv.estimatedBytes();
        }
        if (object.payload instanceof ZSetValue zv) {
            return zv.estimatedBytes();
        }
        return 0;
    }

    static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength)
                + Math.max(0, valueLength)
                + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    static long sumByteLengths(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (byte[] value : values) {
            if (value != null) {
                total += value.length;
            }
        }
        return total;
    }

    static long sumZSetMemberByteLengths(List<byte[]> scoreMemberPairs) {
        long memberBytes = 0L;
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                byte[] member = scoreMemberPairs.get(i);
                if (member != null) {
                    memberBytes += member.length;
                }
            }
        }
        return memberBytes;
    }

    static long estimateCollectionWriteUpperBound(int keyLength, long payloadBytes, long structuralBytes) {
        return estimateStringWriteUpperBound(keyLength, 0)
                + Math.max(0L, payloadBytes)
                + Math.max(0L, structuralBytes);
    }

    static long estimateSetWriteUpperBound(int keyLength, List<byte[]> members) {
        int memberCount = members == null ? 0 : members.size();
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(members),
                Math.multiplyExact((long) memberCount, SET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    static long estimateZSetWriteUpperBound(int keyLength, List<byte[]> scoreMemberPairs) {
        int memberCount = scoreMemberPairs == null ? 0 : scoreMemberPairs.size() / 2;
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumZSetMemberByteLengths(scoreMemberPairs),
                Math.multiplyExact((long) memberCount, ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }
}
