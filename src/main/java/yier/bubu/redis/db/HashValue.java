package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class HashValue implements YierdisValue {
    // Redis stores small hashes in a compact encoding (listpack) and upgrades to hashtable as needed.
    // We approximate that behavior by starting with a single packed blob and upgrading to HashMap.
    private static final int LISTPACK_MAX_ENTRIES = 512;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;

    private byte[] packedBlob = new byte[0];
    private int[] packedOffsets = new int[0];
    private int packedEntries = 0;
    private ByteArrayHashMap<byte[]> map;

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public ValueEncoding encoding() {
        return map != null ? ValueEncoding.HASH_HT : ValueEncoding.HASH_PACKED;
    }

    int size() {
        if (map != null) {
            return map.size();
        }
        return packedEntries;
    }

    int hset(byte[] field, byte[] value) {
        if (map != null) {
            boolean isNew = !map.containsKey(field);
            map.put(field, value);
            return isNew ? 1 : 0;
        }

        if (shouldConvertToHashMap(field, value)) {
            convertToHashMap();
            return hset(field, value);
        }

        int entryIndex = indexOfField(field);
        if (entryIndex >= 0) {
            updateValueAt(entryIndex, field, value);
            return 0;
        }

        appendPair(field, value);
        if (size() > LISTPACK_MAX_ENTRIES) {
            convertToHashMap();
        }
        return 1;
    }

    int hsetMany(List<byte[]> fieldValuePairs) {
        int added = 0;
        for (int i = 0; i < fieldValuePairs.size(); i += 2) {
            byte[] field = fieldValuePairs.get(i);
            byte[] value = fieldValuePairs.get(i + 1);
            added += hset(field, value);
        }
        return added;
    }

    byte[] hget(byte[] field) {
        if (map != null) {
            return map.get(field);
        }
        int entryIndex = indexOfField(field);
        if (entryIndex < 0) {
            return null;
        }

        int pairStart = packedOffsets[entryIndex];
        int p = skipField(pairStart);
        long r = readVarint(packedBlob, p);
        int storedValueLen = (int) r;
        p = (int) (r >>> 32);
        int valueStart = p;
        if (storedValueLen == 0) {
            return null;
        }
        int valueLen = storedValueLen - 1;
        return Arrays.copyOfRange(packedBlob, valueStart, valueStart + valueLen);
    }

    int hdel(List<byte[]> fields) {
        int removed = 0;
        if (map != null) {
            for (byte[] f : fields) {
                if (map.removeKey(f)) {
                    removed++;
                }
            }
            return removed;
        }

        for (byte[] f : fields) {
            int entryIndex = indexOfField(f);
            if (entryIndex < 0) {
                continue;
            }
            removeAt(entryIndex);
            removed++;
        }
        return removed;
    }

    List<byte[]> hgetallPairs() {
        if (map != null) {
            List<byte[]> out = new ArrayList<>(map.size() * 2);
            map.forEach((k, v) -> {
                out.add(k);
                out.add(v);
            });
            return out;
        }

        List<byte[]> out = new ArrayList<>(packedEntries * 2);
        for (int i = 0; i < packedEntries; i++) {
            int p = packedOffsets[i];

            long r = readVarint(packedBlob, p);
            int fieldLen = (int) r;
            p = (int) (r >>> 32);
            int fieldStart = p;
            p += fieldLen;

            r = readVarint(packedBlob, p);
            int storedValueLen = (int) r;
            p = (int) (r >>> 32);
            int valueStart = p;

            out.add(Arrays.copyOfRange(packedBlob, fieldStart, fieldStart + fieldLen));
            if (storedValueLen == 0) {
                out.add(null);
            } else {
                int valueLen = storedValueLen - 1;
                out.add(Arrays.copyOfRange(packedBlob, valueStart, valueStart + valueLen));
            }
        }
        return out;
    }

    private boolean shouldConvertToHashMap(byte[] field, byte[] value) {
        if (size() >= LISTPACK_MAX_ENTRIES) {
            return true;
        }
        return isOversize(field) || isOversize(value);
    }

    private static boolean isOversize(byte[] b) {
        return b != null && b.length > LISTPACK_MAX_ELEMENT_BYTES;
    }

    private int indexOfField(byte[] field) {
        for (int entryIndex = 0; entryIndex < packedEntries; entryIndex++) {
            int p = packedOffsets[entryIndex];
            long r = readVarint(packedBlob, p);
            int fieldLen = (int) r;
            p = (int) (r >>> 32);
            int fieldStart = p;
            if (bytesEqual(packedBlob, fieldStart, fieldLen, field)) {
                return entryIndex;
            }
        }
        return -1;
    }

    private void appendPair(byte[] field, byte[] value) {
        int storedValueLen = value == null ? 0 : value.length + 1;
        int valueLen = storedValueLen == 0 ? 0 : storedValueLen - 1;
        int encodedLen = varintLength(field.length) + field.length + varintLength(storedValueLen) + valueLen;

        int pairStart = packedBlob.length;
        byte[] next = new byte[pairStart + encodedLen];
        System.arraycopy(packedBlob, 0, next, 0, packedBlob.length);

        int p = pairStart;
        p = writeVarint(next, p, field.length);
        System.arraycopy(field, 0, next, p, field.length);
        p += field.length;
        p = writeVarint(next, p, storedValueLen);
        if (storedValueLen != 0) {
            System.arraycopy(value, 0, next, p, valueLen);
        }

        packedBlob = next;
        packedOffsets = Arrays.copyOf(packedOffsets, packedEntries + 1);
        packedOffsets[packedEntries] = pairStart;
        packedEntries++;
    }

    private void updateValueAt(int entryIndex, byte[] field, byte[] value) {
        int pairStart = packedOffsets[entryIndex];

        long r = readVarint(packedBlob, pairStart);
        int fieldLen = (int) r;
        int p = (int) (r >>> 32);
        int fieldStart = p;
        int oldFieldEnd = fieldStart + fieldLen;

        p = oldFieldEnd;
        r = readVarint(packedBlob, p);
        int oldStoredValueLen = (int) r;
        p = (int) (r >>> 32);
        int oldValueStart = p;
        int oldValueLen = oldStoredValueLen == 0 ? 0 : oldStoredValueLen - 1;
        int oldPairEnd = oldValueStart + oldValueLen;

        int storedValueLen = value == null ? 0 : value.length + 1;
        int valueLen = storedValueLen == 0 ? 0 : storedValueLen - 1;
        int newPairLen = varintLength(field.length) + field.length + varintLength(storedValueLen) + valueLen;
        int oldPairLen = oldPairEnd - pairStart;
        int delta = newPairLen - oldPairLen;

        byte[] next = new byte[packedBlob.length + delta];
        System.arraycopy(packedBlob, 0, next, 0, pairStart);

        int w = pairStart;
        w = writeVarint(next, w, field.length);
        System.arraycopy(field, 0, next, w, field.length);
        w += field.length;
        w = writeVarint(next, w, storedValueLen);
        if (storedValueLen != 0) {
            System.arraycopy(value, 0, next, w, valueLen);
            w += valueLen;
        }

        System.arraycopy(packedBlob, oldPairEnd, next, pairStart + newPairLen, packedBlob.length - oldPairEnd);

        packedBlob = next;
        if (delta != 0) {
            for (int i = entryIndex + 1; i < packedEntries; i++) {
                packedOffsets[i] += delta;
            }
        }
    }

    private void removeAt(int entryIndex) {
        int pairStart = packedOffsets[entryIndex];
        int pairEnd = pairEnd(pairStart);
        int removedLen = pairEnd - pairStart;

        byte[] next = new byte[packedBlob.length - removedLen];
        System.arraycopy(packedBlob, 0, next, 0, pairStart);
        System.arraycopy(packedBlob, pairEnd, next, pairStart, packedBlob.length - pairEnd);

        for (int i = entryIndex + 1; i < packedEntries; i++) {
            packedOffsets[i - 1] = packedOffsets[i] - removedLen;
        }
        packedEntries--;
        packedOffsets = Arrays.copyOf(packedOffsets, packedEntries);
        packedBlob = next;
    }

    private int pairEnd(int pairStart) {
        int p = skipField(pairStart);
        long r = readVarint(packedBlob, p);
        int storedValueLen = (int) r;
        p = (int) (r >>> 32);
        int valueLen = storedValueLen == 0 ? 0 : storedValueLen - 1;
        return p + valueLen;
    }

    private int skipField(int pairStart) {
        long r = readVarint(packedBlob, pairStart);
        int fieldLen = (int) r;
        int p = (int) (r >>> 32);
        return p + fieldLen;
    }

    private static boolean bytesEqual(byte[] buf, int bufOff, int len, byte[] other) {
        if (other == null || other.length != len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (buf[bufOff + i] != other[i]) {
                return false;
            }
        }
        return true;
    }

    private static int varintLength(int v) {
        int len = 1;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            len++;
        }
        return len;
    }

    private static int writeVarint(byte[] out, int pos, int v) {
        while ((v & ~0x7F) != 0) {
            out[pos++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        out[pos++] = (byte) v;
        return pos;
    }

    private static long readVarint(byte[] buf, int pos) {
        int result = 0;
        int shift = 0;
        while (true) {
            int b = buf[pos++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalStateException("varint too long");
            }
        }
        return (((long) pos) << 32) | (result & 0xffffffffL);
    }

    private void convertToHashMap() {
        if (map != null) {
            return;
        }
        ByteArrayHashMap<byte[]> out = new ByteArrayHashMap<>(Math.max(16, packedEntries));
        for (int i = 0; i < packedEntries; i++) {
            int p = packedOffsets[i];

            long r = readVarint(packedBlob, p);
            int fieldLen = (int) r;
            p = (int) (r >>> 32);
            int fieldStart = p;
            p += fieldLen;

            r = readVarint(packedBlob, p);
            int storedValueLen = (int) r;
            p = (int) (r >>> 32);
            int valueStart = p;
            int valueLen = storedValueLen == 0 ? 0 : storedValueLen - 1;

            byte[] fieldCopy = Arrays.copyOfRange(packedBlob, fieldStart, fieldStart + fieldLen);
            byte[] valueCopy = storedValueLen == 0 ? null : Arrays.copyOfRange(packedBlob, valueStart, valueStart + valueLen);
            out.put(fieldCopy, valueCopy);
        }
        this.packedBlob = null;
        this.packedOffsets = null;
        this.packedEntries = 0;
        this.map = out;
    }
}
