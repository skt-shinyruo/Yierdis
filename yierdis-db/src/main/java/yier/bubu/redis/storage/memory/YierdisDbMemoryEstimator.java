package yier.bubu.redis.storage.memory;

import java.util.List;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

final class YierdisDbMemoryEstimator {
    private static final long SET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final long ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 96L;

    static long entryMetadataBytes(EntryRecord record) {
        if (record == null) {
            return 0L;
        }
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE
                + (record.type() == ValueType.STRING && record.encoding() == ValueEncoding.STRING_INT
                ? Long.BYTES
                : 0L);
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
                Math.multiplyExact(sumByteLengths(members), 2L),
                Math.multiplyExact((long) memberCount, SET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    static long estimateZSetWriteUpperBound(int keyLength, List<byte[]> scoreMemberPairs) {
        int memberCount = scoreMemberPairs == null ? 0 : scoreMemberPairs.size() / 2;
        long memberBytes = sumZSetMemberByteLengths(scoreMemberPairs);
        return estimateCollectionWriteUpperBound(
                keyLength,
                Math.multiplyExact(memberBytes, 4L),
                Math.multiplyExact((long) memberCount, ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }
}
