package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.util.Arrays;
import java.util.List;

// HyperLogLog（PFADD/PFCOUNT/PFMERGE）实现：以 STRING bytes 存储，并通过固定 header 区分普通 string 与 HLL string。
public final class YierdisHyperLogLog {
    public static final int P = 14;
    public static final int REGISTERS = 1 << P;
    public static final int DENSE_REGISTER_BITS = 6;
    public static final int DENSE_DATA_BYTES = (REGISTERS * DENSE_REGISTER_BITS + 7) / 8;

    public static final int HEADER_BYTES = 8;
    private static final byte[] MAGIC = new byte[]{'H', 'L', 'L', '1'};

    private static final int HEADER_ENCODING_OFFSET = 4;
    private static final int HEADER_P_OFFSET = 5;
    private static final int HEADER_VERSION_OFFSET = 6;

    private static final int VERSION = 1;
    private static final int ENCODING_SPARSE = 0;
    private static final int ENCODING_DENSE = 1;

    private static final int MAX_REGISTER = (1 << DENSE_REGISTER_BITS) - 1;
    private static final int SPARSE_ENTRY_BYTES = 3;

    private YierdisHyperLogLog() {
    }

    public static byte[] newSparse() {
        byte[] out = new byte[HEADER_BYTES];
        writeHeader(out, ENCODING_SPARSE);
        return out;
    }

    public static byte[] newDenseEmpty() {
        byte[] out = new byte[HEADER_BYTES + DENSE_DATA_BYTES];
        writeHeader(out, ENCODING_DENSE);
        return out;
    }

    public static int denseLength() {
        return HEADER_BYTES + DENSE_DATA_BYTES;
    }

    public static int sparseLengthUpperBoundForElements(List<byte[]> elements) {
        int nonEmpty = 0;
        if (elements != null) {
            for (byte[] element : elements) {
                if (element != null && element.length > 0) {
                    nonEmpty++;
                }
            }
        }
        int sparseLength = HEADER_BYTES + nonEmpty * SPARSE_ENTRY_BYTES;
        return Math.max(HEADER_BYTES, Math.min(denseLength(), sparseLength));
    }

    public static boolean isDense(StringRoot root, ValueHandle handle) {
        if (!isHllString(root, handle)) {
            return false;
        }
        return (root.byteAt(handle, HEADER_ENCODING_OFFSET) & 0xFF) == ENCODING_DENSE;
    }

    public static boolean isHllString(StringRoot root, ValueHandle handle) {
        if (root == null || handle == null || root.length(handle) < HEADER_BYTES) {
            return false;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (root.byteAt(handle, i) != MAGIC[i]) {
                return false;
            }
        }
        int p = root.byteAt(handle, HEADER_P_OFFSET) & 0xFF;
        int ver = root.byteAt(handle, HEADER_VERSION_OFFSET) & 0xFF;
        return p == P && ver == VERSION;
    }

    public static boolean pfAdd(StringRoot root, ValueHandle handle, List<byte[]> elements) {
        if (root == null || handle == null) {
            throw new IllegalArgumentException("root and handle must not be null");
        }
        if (!isHllString(root, handle)) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if (elements == null || elements.isEmpty()) {
            return false;
        }

        int enc = root.byteAt(handle, HEADER_ENCODING_OFFSET) & 0xFF;
        if (enc == ENCODING_DENSE) {
            return pfAddDenseInPlace(root, handle, elements);
        }
        if (enc == ENCODING_SPARSE) {
            return pfAddSparseRewrite(root, handle, elements);
        }
        throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    public static void mergeHllIntoRegisters(byte[] raw, int[] registers) {
        if (!isValidHllBytes(raw)) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        int enc = raw[HEADER_ENCODING_OFFSET] & 0xFF;
        if (enc == ENCODING_DENSE) {
            if (raw.length != HEADER_BYTES + DENSE_DATA_BYTES) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            for (int i = 0; i < REGISTERS; i++) {
                int v = denseGetRegister(raw, i);
                if (v > registers[i]) {
                    registers[i] = v;
                }
            }
            return;
        }
        if (enc == ENCODING_SPARSE) {
            int dataLen = raw.length - HEADER_BYTES;
            if (dataLen < 0 || (dataLen % SPARSE_ENTRY_BYTES) != 0) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            for (int pos = HEADER_BYTES; pos < raw.length; pos += SPARSE_ENTRY_BYTES) {
                int idx = ((raw[pos] & 0xFF) << 8) | (raw[pos + 1] & 0xFF);
                int v = raw[pos + 2] & 0xFF;
                if (idx >= REGISTERS || v < 0 || v > MAX_REGISTER) {
                    throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                if (v > registers[idx]) {
                    registers[idx] = v;
                }
            }
            return;
        }
        throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    public static void mergeHllIntoRegisters(BytesSlice raw, int[] registers) {
        if (!isValidHllBytes(raw)) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        int enc = raw.getByte(HEADER_ENCODING_OFFSET) & 0xFF;
        if (enc == ENCODING_DENSE) {
            if (raw.length() != HEADER_BYTES + DENSE_DATA_BYTES) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            for (int i = 0; i < REGISTERS; i++) {
                int v = denseGetRegister(raw, i);
                if (v > registers[i]) {
                    registers[i] = v;
                }
            }
            return;
        }
        if (enc == ENCODING_SPARSE) {
            int dataLen = raw.length() - HEADER_BYTES;
            if (dataLen < 0 || (dataLen % SPARSE_ENTRY_BYTES) != 0) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            for (int pos = HEADER_BYTES; pos < raw.length(); pos += SPARSE_ENTRY_BYTES) {
                int idx = ((raw.getByte(pos) & 0xFF) << 8) | (raw.getByte(pos + 1) & 0xFF);
                int v = raw.getByte(pos + 2) & 0xFF;
                if (idx >= REGISTERS || v < 0 || v > MAX_REGISTER) {
                    throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                if (v > registers[idx]) {
                    registers[idx] = v;
                }
            }
            return;
        }
        throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    public static long estimateCardinality(int[] registers) {
        if (registers == null || registers.length != REGISTERS) {
            throw new IllegalArgumentException("registers must be length " + REGISTERS);
        }

        int zeros = 0;
        double sum = 0.0;
        for (int v : registers) {
            if (v <= 0) {
                zeros++;
                sum += 1.0;
                continue;
            }
            sum += Math.scalb(1.0, -v);
        }

        double m = REGISTERS;
        double alpha = 0.7213 / (1.0 + 1.079 / m);
        double estimate = alpha * m * m / sum;

        // 小范围修正（linear counting）。
        if (estimate <= 2.5 * m && zeros > 0) {
            estimate = m * Math.log(m / zeros);
        }

        if (estimate < 0) {
            return 0;
        }
        return Math.round(estimate);
    }

    public static byte[] denseBytesFromRegisters(int[] registers) {
        if (registers == null || registers.length != REGISTERS) {
            throw new IllegalArgumentException("registers must be length " + REGISTERS);
        }
        byte[] out = newDenseEmpty();
        for (int i = 0; i < REGISTERS; i++) {
            int v = registers[i];
            if (v < 0) {
                v = 0;
            } else if (v > MAX_REGISTER) {
                v = MAX_REGISTER;
            }
            if (v == 0) {
                continue;
            }
            denseSetRegister(out, i, v);
        }
        return out;
    }

    /**
     * 计算 PFADD 的替换表示，不修改当前 native value。
     *
     * @return 替换后的 HLL bytes；寄存器没有变化时返回 {@code null}
     */
    public static byte[] prepareAdd(byte[] current, List<byte[]> elements) {
        byte[] base = current == null ? newSparse() : current;
        if (!isValidHllBytes(base)) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        int[] registers = new int[REGISTERS];
        mergeHllIntoRegisters(base, registers);
        boolean changed = applyElements(registers, elements);
        return changed ? bytesFromRegisters(registers) : null;
    }

    /**
     * 计算 PFMERGE 的 dense 替换表示，不修改目标 key。
     *
     * @return 替换后的 HLL bytes；目标寄存器已经一致时返回 {@code null}
     */
    public static byte[] prepareMerge(byte[] current, int[] mergedRegisters) {
        if (mergedRegisters == null || mergedRegisters.length != REGISTERS) {
            throw new IllegalArgumentException("mergedRegisters must be length " + REGISTERS);
        }
        if (current != null) {
            if (!isValidHllBytes(current)) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            int[] existing = new int[REGISTERS];
            mergeHllIntoRegisters(current, existing);
            if (Arrays.equals(existing, mergedRegisters)) {
                return null;
            }
        }
        return denseBytesFromRegisters(mergedRegisters);
    }

    private static byte[] bytesFromRegisters(int[] registers) {
        int entries = 0;
        for (int value : registers) {
            if (value > 0) {
                entries++;
            }
        }
        int sparseLength = HEADER_BYTES + entries * SPARSE_ENTRY_BYTES;
        if (sparseLength >= denseLength()) {
            return denseBytesFromRegisters(registers);
        }

        byte[] out = new byte[sparseLength];
        writeHeader(out, ENCODING_SPARSE);
        int pos = HEADER_BYTES;
        for (int index = 0; index < REGISTERS; index++) {
            int value = registers[index];
            if (value <= 0) {
                continue;
            }
            out[pos] = (byte) (index >>> 8);
            out[pos + 1] = (byte) index;
            out[pos + 2] = (byte) value;
            pos += SPARSE_ENTRY_BYTES;
        }
        return out;
    }

    private static boolean applyElements(int[] registers, List<byte[]> elements) {
        if (elements == null || elements.isEmpty()) {
            return false;
        }
        boolean changed = false;
        for (byte[] element : elements) {
            if (element == null || element.length == 0) {
                continue;
            }
            long hash = murmurHash3_x64_128_h1(element);
            int registerIndex = (int) (hash & (REGISTERS - 1));
            long word = hash >>> P;
            int rank = (Long.numberOfLeadingZeros(word) + 1) - P;
            if (rank < 1) {
                rank = 1;
            } else if (rank > MAX_REGISTER) {
                rank = MAX_REGISTER;
            }
            if (rank > registers[registerIndex]) {
                registers[registerIndex] = rank;
                changed = true;
            }
        }
        return changed;
    }

    private static void writeHeader(byte[] raw, int encoding) {
        System.arraycopy(MAGIC, 0, raw, 0, MAGIC.length);
        raw[HEADER_ENCODING_OFFSET] = (byte) encoding;
        raw[HEADER_P_OFFSET] = (byte) P;
        raw[HEADER_VERSION_OFFSET] = (byte) VERSION;
        raw[7] = 0;
    }

    private static boolean isValidHllBytes(byte[] raw) {
        if (raw == null || raw.length < HEADER_BYTES) {
            return false;
        }
        if (raw[0] != MAGIC[0] || raw[1] != MAGIC[1] || raw[2] != MAGIC[2] || raw[3] != MAGIC[3]) {
            return false;
        }
        int p = raw[HEADER_P_OFFSET] & 0xFF;
        int ver = raw[HEADER_VERSION_OFFSET] & 0xFF;
        return p == P && ver == VERSION;
    }

    private static boolean isValidHllBytes(BytesSlice raw) {
        if (raw == null || raw.length() < HEADER_BYTES) {
            return false;
        }
        if (raw.getByte(0) != MAGIC[0]
                || raw.getByte(1) != MAGIC[1]
                || raw.getByte(2) != MAGIC[2]
                || raw.getByte(3) != MAGIC[3]) {
            return false;
        }
        int p = raw.getByte(HEADER_P_OFFSET) & 0xFF;
        int ver = raw.getByte(HEADER_VERSION_OFFSET) & 0xFF;
        return p == P && ver == VERSION;
    }

    private static boolean pfAddDenseInPlace(StringRoot root, ValueHandle handle, List<byte[]> elements) {
        if (root.length(handle) != HEADER_BYTES + DENSE_DATA_BYTES) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        boolean changed = false;
        for (byte[] element : elements) {
            if (element == null || element.length == 0) {
                continue;
            }
            long h = murmurHash3_x64_128_h1(element);
            int regIndex = (int) (h & (REGISTERS - 1));
            long w = h >>> P;
            int rank = (Long.numberOfLeadingZeros(w) + 1) - P;
            if (rank < 1) {
                rank = 1;
            } else if (rank > MAX_REGISTER) {
                rank = MAX_REGISTER;
            }

            int current = denseGetRegister(root, handle, regIndex);
            if (rank > current) {
                denseSetRegister(root, handle, regIndex, rank);
                changed = true;
            }
        }
        return changed;
    }

    private static boolean pfAddSparseRewrite(StringRoot root, ValueHandle handle, List<byte[]> elements) {
        byte[] raw = root.copy(handle);
        if (!isValidHllBytes(raw)) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        int enc = raw[HEADER_ENCODING_OFFSET] & 0xFF;
        if (enc != ENCODING_SPARSE) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        int dataLen = raw.length - HEADER_BYTES;
        if (dataLen < 0 || (dataLen % SPARSE_ENTRY_BYTES) != 0) {
            throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        int[] registers = new int[REGISTERS];
        int entries = 0;
        for (int pos = HEADER_BYTES; pos < raw.length; pos += SPARSE_ENTRY_BYTES) {
            int idx = ((raw[pos] & 0xFF) << 8) | (raw[pos + 1] & 0xFF);
            int v = raw[pos + 2] & 0xFF;
            if (idx >= REGISTERS || v < 0 || v > MAX_REGISTER) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            if (registers[idx] == 0) {
                entries++;
            }
            if (v > registers[idx]) {
                registers[idx] = v;
            }
        }

        boolean changed = false;
        for (byte[] element : elements) {
            if (element == null || element.length == 0) {
                continue;
            }
            long h = murmurHash3_x64_128_h1(element);
            int regIndex = (int) (h & (REGISTERS - 1));
            long w = h >>> P;
            int rank = (Long.numberOfLeadingZeros(w) + 1) - P;
            if (rank < 1) {
                rank = 1;
            } else if (rank > MAX_REGISTER) {
                rank = MAX_REGISTER;
            }

            int current = registers[regIndex];
            if (rank > current) {
                if (current == 0) {
                    entries++;
                }
                registers[regIndex] = rank;
                changed = true;
            }
        }

        if (!changed) {
            return false;
        }

        int sparseLen = HEADER_BYTES + entries * SPARSE_ENTRY_BYTES;
        int denseLen = HEADER_BYTES + DENSE_DATA_BYTES;
        if (sparseLen >= denseLen) {
            root.overwrite(handle, denseBytesFromRegisters(registers));
            return true;
        }

        byte[] next = new byte[sparseLen];
        writeHeader(next, ENCODING_SPARSE);
        int pos = HEADER_BYTES;
        for (int idx = 0; idx < REGISTERS; idx++) {
            int v = registers[idx];
            if (v <= 0) {
                continue;
            }
            next[pos] = (byte) (idx >>> 8);
            next[pos + 1] = (byte) idx;
            next[pos + 2] = (byte) v;
            pos += SPARSE_ENTRY_BYTES;
        }
        root.overwrite(handle, next);
        return true;
    }

    private static int denseGetRegister(byte[] raw, int regIndex) {
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = bitPos >>> 3;
        int bitOffset = bitPos & 7;
        int base = HEADER_BYTES + byteIndex;
        int b0 = raw[base] & 0xFF;
        if (bitOffset <= 2) {
            return (b0 >>> bitOffset) & 0x3F;
        }
        int b1 = raw[base + 1] & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int part0 = (b0 >>> bitOffset) & ((1 << bitsInFirst) - 1);
        int part1 = (b1 & ((1 << (DENSE_REGISTER_BITS - bitsInFirst)) - 1)) << bitsInFirst;
        return part0 | part1;
    }

    private static int denseGetRegister(BytesSlice raw, int regIndex) {
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = bitPos >>> 3;
        int bitOffset = bitPos & 7;
        int base = HEADER_BYTES + byteIndex;
        int b0 = raw.getByte(base) & 0xFF;
        if (bitOffset <= 2) {
            return (b0 >>> bitOffset) & 0x3F;
        }
        int b1 = raw.getByte(base + 1) & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int part0 = (b0 >>> bitOffset) & ((1 << bitsInFirst) - 1);
        int part1 = (b1 & ((1 << (DENSE_REGISTER_BITS - bitsInFirst)) - 1)) << bitsInFirst;
        return part0 | part1;
    }

    private static void denseSetRegister(byte[] raw, int regIndex, int value) {
        int v = value & 0x3F;
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = bitPos >>> 3;
        int bitOffset = bitPos & 7;
        int base = HEADER_BYTES + byteIndex;
        int b0 = raw[base] & 0xFF;
        if (bitOffset <= 2) {
            int mask = 0x3F << bitOffset;
            raw[base] = (byte) ((b0 & ~mask) | (v << bitOffset));
            return;
        }
        int b1 = raw[base + 1] & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int loMask = (1 << bitsInFirst) - 1;
        int hiBits = DENSE_REGISTER_BITS - bitsInFirst;
        int hiMask = (1 << hiBits) - 1;

        int part0Mask = loMask << bitOffset;
        int nextB0 = (b0 & ~part0Mask) | ((v & loMask) << bitOffset);
        int nextB1 = (b1 & ~hiMask) | ((v >>> bitsInFirst) & hiMask);
        raw[base] = (byte) nextB0;
        raw[base + 1] = (byte) nextB1;
    }

    private static int denseGetRegister(StringRoot root, ValueHandle handle, int regIndex) {
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = bitPos >>> 3;
        int bitOffset = bitPos & 7;
        int base = HEADER_BYTES + byteIndex;
        int b0 = root.byteAt(handle, base) & 0xFF;
        if (bitOffset <= 2) {
            return (b0 >>> bitOffset) & 0x3F;
        }
        int b1 = root.byteAt(handle, base + 1) & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int part0 = (b0 >>> bitOffset) & ((1 << bitsInFirst) - 1);
        int part1 = (b1 & ((1 << (DENSE_REGISTER_BITS - bitsInFirst)) - 1)) << bitsInFirst;
        return part0 | part1;
    }

    private static void denseSetRegister(StringRoot root, ValueHandle handle, int regIndex, int value) {
        int v = value & 0x3F;
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = bitPos >>> 3;
        int bitOffset = bitPos & 7;
        int base = HEADER_BYTES + byteIndex;
        int b0 = root.byteAt(handle, base) & 0xFF;
        if (bitOffset <= 2) {
            int mask = 0x3F << bitOffset;
            root.setByteAt(handle, base, (byte) ((b0 & ~mask) | (v << bitOffset)));
            return;
        }
        int b1 = root.byteAt(handle, base + 1) & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int loMask = (1 << bitsInFirst) - 1;
        int hiBits = DENSE_REGISTER_BITS - bitsInFirst;
        int hiMask = (1 << hiBits) - 1;

        int part0Mask = loMask << bitOffset;
        int nextB0 = (b0 & ~part0Mask) | ((v & loMask) << bitOffset);
        int nextB1 = (b1 & ~hiMask) | ((v >>> bitsInFirst) & hiMask);
        root.setByteAt(handle, base, (byte) nextB0);
        root.setByteAt(handle, base + 1, (byte) nextB1);
    }

    // MurmurHash3 x64 128-bit 的 h1（返回 64-bit），用于 HLL 的 index/rank 计算。
    private static long murmurHash3_x64_128_h1(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }
        int len = data.length;
        final long c1 = 0x87c37b91114253d5L;
        final long c2 = 0x4cf5ad432745937fL;
        long h1 = 0L;
        long h2 = 0L;

        int nblocks = len / 16;
        for (int i = 0; i < nblocks; i++) {
            int base = i * 16;
            long k1 = getLongLE(data, base);
            long k2 = getLongLE(data, base + 8);

            k1 *= c1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        long k1 = 0L;
        long k2 = 0L;
        int tailStart = nblocks * 16;
        int tail = len & 15;
        switch (tail) {
            case 15:
                k2 ^= ((long) data[tailStart + 14] & 0xFF) << 48;
            case 14:
                k2 ^= ((long) data[tailStart + 13] & 0xFF) << 40;
            case 13:
                k2 ^= ((long) data[tailStart + 12] & 0xFF) << 32;
            case 12:
                k2 ^= ((long) data[tailStart + 11] & 0xFF) << 24;
            case 11:
                k2 ^= ((long) data[tailStart + 10] & 0xFF) << 16;
            case 10:
                k2 ^= ((long) data[tailStart + 9] & 0xFF) << 8;
            case 9:
                k2 ^= ((long) data[tailStart + 8] & 0xFF);
                k2 *= c2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= c1;
                h2 ^= k2;
            case 8:
                k1 ^= ((long) data[tailStart + 7] & 0xFF) << 56;
            case 7:
                k1 ^= ((long) data[tailStart + 6] & 0xFF) << 48;
            case 6:
                k1 ^= ((long) data[tailStart + 5] & 0xFF) << 40;
            case 5:
                k1 ^= ((long) data[tailStart + 4] & 0xFF) << 32;
            case 4:
                k1 ^= ((long) data[tailStart + 3] & 0xFF) << 24;
            case 3:
                k1 ^= ((long) data[tailStart + 2] & 0xFF) << 16;
            case 2:
                k1 ^= ((long) data[tailStart + 1] & 0xFF) << 8;
            case 1:
                k1 ^= ((long) data[tailStart] & 0xFF);
                k1 *= c1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= c2;
                h1 ^= k1;
            default:
        }

        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        // h2 += h1; // 我们只需要 h1
        return h1;
    }

    private static long getLongLE(byte[] data, int off) {
        return ((long) data[off] & 0xFF)
                | (((long) data[off + 1] & 0xFF) << 8)
                | (((long) data[off + 2] & 0xFF) << 16)
                | (((long) data[off + 3] & 0xFF) << 24)
                | (((long) data[off + 4] & 0xFF) << 32)
                | (((long) data[off + 5] & 0xFF) << 40)
                | (((long) data[off + 6] & 0xFF) << 48)
                | (((long) data[off + 7] & 0xFF) << 56);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
