package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmByteMap;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBytesRef;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBytesRefSlice;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmListpack;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.ArrayList;
import java.util.List;

public final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with small parallel arrays (packed) and upgrading to a hash map.

    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final YierdisFfmBlobStore ffmBlobStore;

    // Packed form uses a listpack-like contiguous buffer containing [field][value] pairs.
    // This preserves binary-safe semantics while avoiding per-entry byte[] objects.
    private YierdisListpack packed;
    private YierdisFfmListpack packedFfm;

    private ByteArrayHashMap<byte[]> map;
    private YierdisFfmByteMap<YierdisFfmBytesRef> mapFfm;
    private long rawBytes;

    public HashValue() {
        this.memoryRuntime = null;
        this.ffmBlobStore = null;
        this.packed = new YierdisListpack();
    }

    public HashValue(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
        this.ffmBlobStore = new YierdisFfmBlobStore(memoryRuntime, "hash");
        this.packedFfm = new YierdisFfmListpack(ffmBlobStore);
    }

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public ValueEncoding encoding() {
        if (memoryRuntime != null) {
            return mapFfm != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
        }
        return map != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
    }

    public int size() {
        if (memoryRuntime != null) {
            if (mapFfm != null) {
                return mapFfm.size();
            }
            return packedFfm.size() / 2;
        }
        if (map != null) {
            return map.size();
        }
        return packed.size() / 2;
    }

    public int hset(byte[] field, byte[] value) {
        if (memoryRuntime != null) {
            if (mapFfm != null) {
                YierdisFfmBytesRef nextValue = value == null ? null : ffmBlobStore.store(value);
                boolean ok = false;
                try {
                    YierdisFfmBytesRef old = mapFfm.put(field, nextValue);
                    ok = true;
                    if (old == null) {
                        rawBytes += (long) field.length + (value == null ? 0 : value.length);
                        return 1;
                    }
                    rawBytes += (value == null ? 0 : value.length) - old.length();
                    ffmBlobStore.release(old);
                    return 0;
                } finally {
                    if (!ok && nextValue != null) {
                        ffmBlobStore.release(nextValue);
                    }
                }
            }

            int pairIndex = indexOfFieldPairFfm(field);
            if (pairIndex >= 0) {
                if (isOversize(value)) {
                    convertToFfmMap();
                    return hset(field, value);
                }
                packedFfm.set(pairIndex + 1, value);
                return 0;
            }

            if (packedFfm.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES
                    || isOversize(field)
                    || isOversize(value)) {
                convertToFfmMap();
                return hset(field, value);
            }

            packedFfm.addLast(field);
            packedFfm.addLast(value);

            if (packedFfm.size() / 2 > YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
                convertToFfmMap();
            }
            return 1;
        }

        if (map != null) {
            byte[] old = map.put(field, value);
            if (old == null) {
                rawBytes += (long) field.length + value.length;
                return 1;
            }
            rawBytes += value.length - old.length;
            return 0;
        }

        int pairIndex = indexOfFieldPair(field);
        if (pairIndex >= 0) {
            if (isOversize(value)) {
                convertToHashMap();
                return hset(field, value);
            }
            // Replace value at (pairIndex + 1).
            packed.set(pairIndex + 1, value);
            return 0;
        }

        // New field: only entry-count based upgrades should consider the insertion.
        if (packed.size() / 2 >= YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES
                || isOversize(field)
                || isOversize(value)) {
            convertToHashMap();
            return hset(field, value);
        }

        packed.addLast(field);
        packed.addLast(value);

        if (packed.size() / 2 > YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES) {
            convertToHashMap();
        }
        return 1;
    }

    public int hsetMany(List<byte[]> fieldValuePairs) {
        int added = 0;
        for (int i = 0; i < fieldValuePairs.size(); i += 2) {
            byte[] field = fieldValuePairs.get(i);
            byte[] value = fieldValuePairs.get(i + 1);
            added += hset(field, value);
        }
        return added;
    }

    public byte[] hget(byte[] field) {
        if (memoryRuntime != null) {
            if (mapFfm != null) {
                YierdisFfmBytesRef ref = mapFfm.get(field);
                return ref == null ? null : ffmBlobStore.toByteArray(ref);
            }
            int pairIndex = indexOfFieldPairFfm(field);
            if (pairIndex < 0) {
                return null;
            }
            return packedFfm.get(pairIndex + 1);
        }
        if (map != null) {
            return map.get(field);
        }
        int pairIndex = indexOfFieldPair(field);
        if (pairIndex < 0) {
            return null;
        }
        return packed.get(pairIndex + 1);
    }

    public int hdel(List<byte[]> fields) {
        int removed = 0;
        if (memoryRuntime != null) {
            if (mapFfm != null) {
                for (byte[] field : fields) {
                    YierdisFfmBytesRef old = mapFfm.remove(field);
                    if (old == null) {
                        continue;
                    }
                    rawBytes -= (long) field.length + old.length();
                    ffmBlobStore.release(old);
                    removed++;
                }
                return removed;
            }

            for (byte[] field : fields) {
                int pairIndex = indexOfFieldPairFfm(field);
                if (pairIndex < 0) {
                    continue;
                }
                packedFfm.removeAt(pairIndex + 1);
                packedFfm.removeAt(pairIndex);
                removed++;
            }
            return removed;
        }

        if (map != null) {
            for (byte[] f : fields) {
                byte[] old = map.remove(f);
                if (old != null) {
                    rawBytes -= (long) f.length + old.length;
                    removed++;
                }
            }
            return removed;
        }

        for (byte[] f : fields) {
            int pairIndex = indexOfFieldPair(f);
            if (pairIndex < 0) {
                continue;
            }
            // Remove value then field to keep indices stable.
            packed.removeAt(pairIndex + 1);
            packed.removeAt(pairIndex);
            removed++;
        }
        return removed;
    }

    public List<byte[]> hgetallPairs() {
        if (memoryRuntime != null) {
            if (mapFfm != null) {
                List<byte[]> out = new ArrayList<>(mapFfm.size() * 2);
                mapFfm.forEach((fieldRef, valueRef) -> {
                    out.add(ffmBlobStore.toByteArray(fieldRef));
                    out.add(valueRef == null ? null : ffmBlobStore.toByteArray(valueRef));
                });
                return out;
            }

            int pairs = packedFfm.size() / 2;
            List<byte[]> out = new ArrayList<>(pairs * 2);
            for (int i = 0; i < packedFfm.size(); i++) {
                out.add(packedFfm.get(i));
            }
            return out;
        }
        if (map != null) {
            List<byte[]> out = new ArrayList<>(map.size() * 2);
            map.forEach((k, v) -> {
                out.add(k);
                out.add(v);
            });
            return out;
        }

        int pairs = packed.size() / 2;
        List<byte[]> out = new ArrayList<>(pairs * 2);
        for (int i = 0; i < packed.size(); i++) {
            out.add(packed.get(i));
        }
        return out;
    }

    public int hgetallCount() {
        return size() * 2;
    }

    public void hgetallPairsInto(BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (memoryRuntime != null) {
            if (mapFfm != null) {
                mapFfm.forEach((fieldRef, valueRef) -> {
                    out.bulkString(new YierdisFfmBytesRefSlice(fieldRef));
                    if (valueRef == null) {
                        out.bulkStringNull();
                    } else {
                        out.bulkString(new YierdisFfmBytesRefSlice(valueRef));
                    }
                });
                return;
            }

            YierdisFfmListpack.Cursor c = packedFfm.cursor();
            while (c.next()) {
                c.writeTo(out);
            }
            return;
        }

        if (map != null) {
            map.forEach((k, v) -> {
                out.bulkString(k, 0, k.length);
                if (v == null) {
                    out.bulkStringNull();
                } else {
                    out.bulkString(v, 0, v.length);
                }
            });
            return;
        }

        YierdisListpack.Cursor c = packed.cursor();
        while (c.next()) {
            c.writeTo(out);
        }
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > YierdisEncodingThresholds.HASH_MAX_LISTPACK_VALUE_BYTES;
    }

    private int indexOfFieldPair(byte[] field) {
        // Packed contains pairs: [field0][value0][field1][value1]...
        int idx = 0;
        YierdisListpack.Cursor c = packed.cursor();
        while (c.next()) {
            if ((idx & 1) == 0 && c.equalsBytes(field)) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    private int indexOfFieldPairFfm(byte[] field) {
        int idx = 0;
        YierdisFfmListpack.Cursor c = packedFfm.cursor();
        while (c.next()) {
            if ((idx & 1) == 0 && c.equalsBytes(field)) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        int pairs = packed.size() / 2;
        ByteArrayHashMap<byte[]> out = new ByteArrayHashMap<>(Math.max(16, pairs));
        for (int i = 0; i + 1 < packed.size(); i += 2) {
            out.put(packed.get(i), packed.get(i + 1));
        }
        rawBytes = packed.rawBytesSize();
        packed.clear();
        this.map = out;
    }

    public long estimatedBytes() {
        if (memoryRuntime != null) {
            return ffmBlobStore.liveBytes();
        }
        if (map != null) {
            return rawBytes + map.estimatedBytes();
        }
        return packed.allocatedBytes();
    }

    @Override
    public void close() {
        if (memoryRuntime != null) {
            if (mapFfm != null) {
                mapFfm.forEach((fieldRef, valueRef) -> {
                    if (valueRef != null) {
                        ffmBlobStore.release(valueRef);
                    }
                });
                mapFfm.close();
                mapFfm = null;
            }
            if (packedFfm != null) {
                packedFfm.close();
                packedFfm = null;
            }
            return;
        }
    }

    private void convertToFfmMap() {
        if (mapFfm != null) {
            return;
        }
        YierdisFfmByteMap<YierdisFfmBytesRef> out = new YierdisFfmByteMap<>(ffmBlobStore);
        long nextRawBytes = 0L;
        boolean ok = false;
        try {
            for (int i = 0; i + 1 < packedFfm.size(); i += 2) {
                byte[] field = packedFfm.get(i);
                byte[] value = packedFfm.get(i + 1);
                YierdisFfmBytesRef nextValue = value == null ? null : ffmBlobStore.store(value);
                boolean inserted = false;
                try {
                    out.put(field, nextValue);
                    inserted = true;
                } finally {
                    if (!inserted && nextValue != null) {
                        ffmBlobStore.release(nextValue);
                    }
                }
                nextRawBytes += (long) field.length + (value == null ? 0 : value.length);
            }
            ok = true;
        } finally {
            if (!ok) {
                out.forEach((fieldRef, valueRef) -> {
                    if (valueRef != null) {
                        ffmBlobStore.release(valueRef);
                    }
                });
                out.close();
            }
        }

        packedFfm.close();
        packedFfm = null;
        rawBytes = nextRawBytes;
        mapFfm = out;
    }
}
