package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.util.Arrays;
import java.util.List;

// HyperLogLog（PFADD/PFCOUNT/PFMERGE）实现：以 STRING bytes 存储，payload 使用 Redis 的
// "HYLL" header 与 sparse/dense 编码（redis/src/hyperloglog.c），因此对相同 member 给出
// 与 Redis 相同的寄存器状态与基数估计。仅内存对齐；不读取旧的 Yierdis 私有 HLL1 payload。
//
// Redis 格式要点：
// - 16 字节 header："HYLL" magic + 1 字节 encoding（0=dense，1=sparse）+ 3 字节保留（0）
//   + 8 字节 little-endian 基数缓存（最高字节的 MSB 置位表示缓存失效）。Yierdis 不缓存基数，
//   写出的 payload 一律是「已失效缓存」状态，与 Redis PFADD 之后、下一次 PFCOUNT 之前的字节一致。
// - dense：16384 个 6-bit 寄存器按 LSB 优先打包，总长 16 + 12288 = 12304 字节。
// - sparse：ZERO（00xxxxxx，1..64 个 0）/ XZERO（01xxxxxx yyyyyyyy，1..16384 个 0）/
//   VAL（1vvvvvxx，值 1..32 重复 1..4 次）三种 opcode 的游程编码。
// - 哈希：MurmurHash64A（seed 0xadc83b19）；低 14 bit 为寄存器下标，其余位统计末尾 0 游程 +1。
// - 估计：Ertl 的 tau/sigma 修正（Redis hllCount），替代旧的 alpha+linear-counting 估计器。
// - sparse 晋升 dense 的条件与 Redis 一致：寄存器值 > 32，或 sparse 长度超过 3000 字节
//   （Redis server.hll_sparse_max_bytes 默认值）。
public final class YierdisHyperLogLog {
    public static final int P = 14;
    public static final int REGISTERS = 1 << P;
    public static final int DENSE_REGISTER_BITS = 6;
    public static final int DENSE_DATA_BYTES = (REGISTERS * DENSE_REGISTER_BITS + 7) / 8;

    public static final int HEADER_BYTES = 16;
    private static final byte[] MAGIC = new byte[]{'H', 'Y', 'L', 'L'};

    private static final int HEADER_ENCODING_OFFSET = 4;
    private static final int HEADER_CARD_OFFSET = 8;

    private static final int ENCODING_DENSE = 0;
    private static final int ENCODING_SPARSE = 1;

    private static final int HLL_Q = 64 - P;
    private static final int MAX_REGISTER = (1 << DENSE_REGISTER_BITS) - 1;

    private static final int SPARSE_ZERO_MAX_LEN = 64;
    private static final int SPARSE_XZERO_MAX_LEN = 16384;
    private static final int SPARSE_VAL_MAX_VALUE = 32;
    private static final int SPARSE_VAL_MAX_LEN = 4;

    // Redis server.hll_sparse_max_bytes 默认值；超过即晋升 dense。
    public static final int SPARSE_MAX_BYTES = 3000;

    private static final long HASH_SEED = 0xadc83b19L;
    private static final double ALPHA_INF = 0.721347520444481703680; // 0.5/ln(2)

    // 空 sparse HLL 的字节数：header + 单个 XZERO:16384。
    private static final int SPARSE_EMPTY_BYTES = HEADER_BYTES + 2;

    // 与 Redis isHLLObjectOrReply 的文案对齐：stored string 不是合法 HLL（magic/encoding/长度校验失败）时的统一文案。
    // 注意 Redis 另有 INVALIDOBJ 错误，只用于 header 合法但 sparse 内容损坏的负载，这里不涉及。
    public static final String INVALID_HLL_ERROR = "WRONGTYPE Key is not a valid HyperLogLog string value.";

    private YierdisHyperLogLog() {
    }

    /** 与 Redis createHLLObject 一致：空 sparse（XZERO:16384），基数缓存为有效的 0。 */
    public static byte[] newSparse() {
        byte[] out = new byte[SPARSE_EMPTY_BYTES];
        writeHeader(out, ENCODING_SPARSE, false);
        writeXzero(out, HEADER_BYTES, REGISTERS);
        return out;
    }

    public static byte[] newDenseEmpty() {
        byte[] out = new byte[HEADER_BYTES + DENSE_DATA_BYTES];
        writeHeader(out, ENCODING_DENSE, false);
        return out;
    }

    public static int denseLength() {
        return HEADER_BYTES + DENSE_DATA_BYTES;
    }

    public static boolean isDense(StringRoot root, ValueHandle handle) {
        if (!isHllString(root, handle)) {
            return false;
        }
        return (root.byteAt(handle, HEADER_ENCODING_OFFSET) & 0xFF) == ENCODING_DENSE;
    }

    /** Redis isHLLObjectOrReply 的 header 级校验：magic、encoding 取值、dense 精确长度。 */
    public static boolean isHllString(StringRoot root, ValueHandle handle) {
        if (root == null || handle == null || root.length(handle) < HEADER_BYTES) {
            return false;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (root.byteAt(handle, i) != MAGIC[i]) {
                return false;
            }
        }
        int encoding = root.byteAt(handle, HEADER_ENCODING_OFFSET) & 0xFF;
        if (encoding == ENCODING_DENSE) {
            return root.length(handle) == denseLength();
        }
        return encoding == ENCODING_SPARSE;
    }

    public static boolean isDenseBytes(byte[] raw) {
        return raw != null
                && raw.length >= HEADER_BYTES
                && (raw[HEADER_ENCODING_OFFSET] & 0xFF) == ENCODING_DENSE;
    }

    public static void mergeHllIntoRegisters(byte[] raw, int[] registers) {
        if (!isValidHllBytes(raw)) {
            throw new YierdisCommandException(INVALID_HLL_ERROR);
        }
        int enc = raw[HEADER_ENCODING_OFFSET] & 0xFF;
        if (enc == ENCODING_DENSE) {
            for (int i = 0; i < REGISTERS; i++) {
                int v = denseGetRegister(raw, i);
                if (v > registers[i]) {
                    registers[i] = v;
                }
            }
            return;
        }
        mergeSparseIntoRegisters(new ByteArrayCursor(raw), registers);
    }

    public static void mergeHllIntoRegisters(BytesSlice raw, int[] registers) {
        if (!isValidHllBytes(raw)) {
            throw new YierdisCommandException(INVALID_HLL_ERROR);
        }
        int enc = raw.getByte(HEADER_ENCODING_OFFSET) & 0xFF;
        if (enc == ENCODING_DENSE) {
            for (int i = 0; i < REGISTERS; i++) {
                int v = denseGetRegister(raw, i);
                if (v > registers[i]) {
                    registers[i] = v;
                }
            }
            return;
        }
        mergeSparseIntoRegisters(new SliceCursor(raw), registers);
    }

    /**
     * Redis hllCount 的 Ertl tau/sigma 估计器：直方图 + 小范围 sigma 修正 + 大范围 tau 修正。
     */
    public static long estimateCardinality(int[] registers) {
        if (registers == null || registers.length != REGISTERS) {
            throw new IllegalArgumentException("registers must be length " + REGISTERS);
        }

        int[] reghisto = new int[MAX_REGISTER + 1];
        for (int v : registers) {
            if (v < 0) {
                v = 0;
            } else if (v > MAX_REGISTER) {
                v = MAX_REGISTER;
            }
            reghisto[v]++;
        }

        double m = REGISTERS;
        double z = m * hllTau((m - reghisto[HLL_Q + 1]) / m);
        for (int j = HLL_Q; j >= 1; j--) {
            z += reghisto[j];
            z *= 0.5;
        }
        z += m * hllSigma(reghisto[0] / m);
        return Math.round(ALPHA_INF * m * m / z);
    }

    public static byte[] denseBytesFromRegisters(int[] registers) {
        if (registers == null || registers.length != REGISTERS) {
            throw new IllegalArgumentException("registers must be length " + REGISTERS);
        }
        byte[] out = new byte[HEADER_BYTES + DENSE_DATA_BYTES];
        writeHeader(out, ENCODING_DENSE, true);
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
     * 空 member 与 Redis 一样参与哈希（MurmurHash64A 对空输入有定义）。
     *
     * @return 替换后的 HLL bytes；寄存器没有变化时返回 {@code null}
     */
    public static byte[] prepareAdd(byte[] current, List<byte[]> elements) {
        boolean denseFloor = false;
        int[] registers = new int[REGISTERS];
        if (current != null) {
            if (!isValidHllBytes(current)) {
                throw new YierdisCommandException(INVALID_HLL_ERROR);
            }
            denseFloor = isDenseBytes(current);
            mergeHllIntoRegisters(current, registers);
        }
        boolean changed = applyElements(registers, elements);
        return changed ? serialize(registers, denseFloor) : null;
    }

    /**
     * 计算 PFMERGE 的替换表示，不修改目标 key。
     * Redis pfmergeCommand 的目标编码规则：任一参与方（含已存在的 dest）是 dense 则结果为 dense，
     * 否则保持 sparse 并按晋升规则升格。
     *
     * @param current        目标 key 当前 payload（不存在时为 {@code null}）
     * @param anySourceDense 任一 source 为 dense 时为 true
     * @return 替换后的 HLL bytes；目标 payload 与结果逐字节一致时返回 {@code null}
     */
    public static byte[] prepareMerge(byte[] current, boolean anySourceDense, int[] mergedRegisters) {
        if (mergedRegisters == null || mergedRegisters.length != REGISTERS) {
            throw new IllegalArgumentException("mergedRegisters must be length " + REGISTERS);
        }
        boolean denseFloor = anySourceDense;
        if (current != null) {
            if (!isValidHllBytes(current)) {
                throw new YierdisCommandException(INVALID_HLL_ERROR);
            }
            if (isDenseBytes(current)) {
                denseFloor = true;
            }
        }
        // 序列化是确定性的（寄存器 + 编码唯一决定字节），逐字节相等即无需重写。
        byte[] replacement = serialize(mergedRegisters, denseFloor);
        return current != null && Arrays.equals(current, replacement) ? null : replacement;
    }

    /**
     * 按 Redis 规则选择编码：floor 为 dense 则 dense；否则能 sparse（寄存器值 ≤ 32 且
     * 长度 ≤ SPARSE_MAX_BYTES）就 sparse，否则晋升 dense。dense 永不降级为 sparse。
     */
    private static byte[] serialize(int[] registers, boolean denseFloor) {
        if (!denseFloor) {
            byte[] sparse = sparseBytesFromRegisters(registers);
            if (sparse != null) {
                return sparse;
            }
        }
        return denseBytesFromRegisters(registers);
    }

    private static boolean applyElements(int[] registers, List<byte[]> elements) {
        if (elements == null || elements.isEmpty()) {
            return false;
        }
        boolean changed = false;
        for (byte[] element : elements) {
            if (element == null) {
                continue;
            }
            long hash = murmurHash64A(element);
            int registerIndex = (int) (hash & (REGISTERS - 1));
            // Redis hllPatLen：去掉下标位后置上终止位，数末尾 0 游程 +1，最大 Q+1。
            long word = hash >>> P;
            word |= 1L << HLL_Q;
            int rank = Long.numberOfTrailingZeros(word) + 1;
            if (rank > registers[registerIndex]) {
                registers[registerIndex] = rank;
                changed = true;
            }
        }
        return changed;
    }

    /**
     * 从寄存器构建 Redis sparse 表示；寄存器值超过 32 或结果超过 SPARSE_MAX_BYTES 时返回
     * {@code null}（调用方晋升 dense）。游程编码规则与 Redis opcode 一致：0 游程 ≤ 64 用
     * ZERO，更长用 XZERO；相同非零值游程按每段 ≤ 4 拆成 VAL。
     */
    private static byte[] sparseBytesFromRegisters(int[] registers) {
        byte[] out = new byte[SPARSE_MAX_BYTES];
        int pos = HEADER_BYTES;
        int index = 0;
        while (index < REGISTERS) {
            int value = registers[index];
            if (value == 0) {
                int run = 1;
                while (index + run < REGISTERS && registers[index + run] == 0) {
                    run++;
                }
                int left = run;
                while (left > SPARSE_ZERO_MAX_LEN) {
                    int chunk = Math.min(left, SPARSE_XZERO_MAX_LEN);
                    if (pos + 2 > out.length) {
                        return null;
                    }
                    writeXzero(out, pos, chunk);
                    pos += 2;
                    left -= chunk;
                }
                if (left > 0) {
                    if (pos + 1 > out.length) {
                        return null;
                    }
                    out[pos++] = (byte) (left - 1);
                }
                index += run;
            } else {
                if (value > SPARSE_VAL_MAX_VALUE) {
                    return null;
                }
                int run = 1;
                while (index + run < REGISTERS && registers[index + run] == value) {
                    run++;
                }
                int left = run;
                while (left > 0) {
                    int chunk = Math.min(left, SPARSE_VAL_MAX_LEN);
                    if (pos + 1 > out.length) {
                        return null;
                    }
                    out[pos++] = (byte) (0x80 | ((value - 1) << 2) | (chunk - 1));
                    left -= chunk;
                }
                index += run;
            }
        }
        byte[] result = pos == out.length ? out : Arrays.copyOf(out, pos);
        writeHeader(result, ENCODING_SPARSE, true);
        return result;
    }

    private static void writeXzero(byte[] out, int pos, int len) {
        out[pos] = (byte) (0x40 | ((len - 1) >>> 8));
        out[pos + 1] = (byte) ((len - 1) & 0xFF);
    }

    /** sparse 游程解码并 max 进寄存器；结构不合法（游程越界/总寄存器数不为 16384）时报 WRONGTYPE。 */
    private static void mergeSparseIntoRegisters(Cursor cursor, int[] registers) {
        int index = 0;
        while (cursor.pos < cursor.length) {
            int b = cursor.get() & 0xFF;
            if ((b & 0xC0) == 0) {
                // ZERO
                index += (b & 0x3F) + 1;
                cursor.pos += 1;
            } else if ((b & 0xC0) == 0x40) {
                // XZERO
                if (cursor.pos + 1 >= cursor.length) {
                    throw new YierdisCommandException(INVALID_HLL_ERROR);
                }
                int len = (((b & 0x3F) << 8) | (cursor.get(cursor.pos + 1) & 0xFF)) + 1;
                index += len;
                cursor.pos += 2;
            } else {
                // VAL
                int value = ((b >>> 2) & 0x1F) + 1;
                int len = (b & 0x3) + 1;
                for (int k = 0; k < len; k++) {
                    if (index >= REGISTERS) {
                        throw new YierdisCommandException(INVALID_HLL_ERROR);
                    }
                    if (value > registers[index]) {
                        registers[index] = value;
                    }
                    index++;
                }
                cursor.pos += 1;
            }
            if (index > REGISTERS) {
                throw new YierdisCommandException(INVALID_HLL_ERROR);
            }
        }
        if (index != REGISTERS) {
            throw new YierdisCommandException(INVALID_HLL_ERROR);
        }
    }

    private static void writeHeader(byte[] raw, int encoding, boolean invalidateCardCache) {
        System.arraycopy(MAGIC, 0, raw, 0, MAGIC.length);
        raw[HEADER_ENCODING_OFFSET] = (byte) encoding;
        // 保留字节 5..7 与基数缓存 8..15 已为 0；缓存最高字节 MSB 置位表示失效（Redis HLL_INVALIDATE_CACHE）。
        if (invalidateCardCache) {
            raw[HEADER_CARD_OFFSET + 7] = (byte) 0x80;
        }
    }

    private static boolean isValidHllBytes(byte[] raw) {
        if (raw == null || raw.length < HEADER_BYTES) {
            return false;
        }
        if (raw[0] != MAGIC[0] || raw[1] != MAGIC[1] || raw[2] != MAGIC[2] || raw[3] != MAGIC[3]) {
            return false;
        }
        int encoding = raw[HEADER_ENCODING_OFFSET] & 0xFF;
        if (encoding == ENCODING_DENSE) {
            return raw.length == denseLength();
        }
        return encoding == ENCODING_SPARSE;
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
        int encoding = raw.getByte(HEADER_ENCODING_OFFSET) & 0xFF;
        if (encoding == ENCODING_DENSE) {
            return raw.length() == denseLength();
        }
        return encoding == ENCODING_SPARSE;
    }

    // Redis HLL_DENSE_GET_REGISTER / HLL_DENSE_SET_REGISTER：6-bit 寄存器 LSB 优先打包。
    // 最后一个寄存器（下标 16383）恰好落在最后一个字节的 bit 2..7，不会越界读 b1。
    private static int denseGetRegister(byte[] raw, int regIndex) {
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = HEADER_BYTES + (bitPos >>> 3);
        int bitOffset = bitPos & 7;
        int b0 = raw[byteIndex] & 0xFF;
        if (bitOffset <= 2) {
            return (b0 >>> bitOffset) & 0x3F;
        }
        int b1 = raw[byteIndex + 1] & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int part0 = (b0 >>> bitOffset) & ((1 << bitsInFirst) - 1);
        int part1 = (b1 & ((1 << (DENSE_REGISTER_BITS - bitsInFirst)) - 1)) << bitsInFirst;
        return part0 | part1;
    }

    private static int denseGetRegister(BytesSlice raw, int regIndex) {
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = HEADER_BYTES + (bitPos >>> 3);
        int bitOffset = bitPos & 7;
        int b0 = raw.getByte(byteIndex) & 0xFF;
        if (bitOffset <= 2) {
            return (b0 >>> bitOffset) & 0x3F;
        }
        int b1 = raw.getByte(byteIndex + 1) & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int part0 = (b0 >>> bitOffset) & ((1 << bitsInFirst) - 1);
        int part1 = (b1 & ((1 << (DENSE_REGISTER_BITS - bitsInFirst)) - 1)) << bitsInFirst;
        return part0 | part1;
    }

    private static void denseSetRegister(byte[] raw, int regIndex, int value) {
        int v = value & 0x3F;
        int bitPos = regIndex * DENSE_REGISTER_BITS;
        int byteIndex = HEADER_BYTES + (bitPos >>> 3);
        int bitOffset = bitPos & 7;
        int b0 = raw[byteIndex] & 0xFF;
        if (bitOffset <= 2) {
            int mask = 0x3F << bitOffset;
            raw[byteIndex] = (byte) ((b0 & ~mask) | (v << bitOffset));
            return;
        }
        int b1 = raw[byteIndex + 1] & 0xFF;
        int bitsInFirst = 8 - bitOffset;
        int loMask = (1 << bitsInFirst) - 1;
        int hiBits = DENSE_REGISTER_BITS - bitsInFirst;
        int hiMask = (1 << hiBits) - 1;

        int part0Mask = loMask << bitOffset;
        int nextB0 = (b0 & ~part0Mask) | ((v & loMask) << bitOffset);
        int nextB1 = (b1 & ~hiMask) | ((v >>> bitsInFirst) & hiMask);
        raw[byteIndex] = (byte) nextB0;
        raw[byteIndex + 1] = (byte) nextB1;
    }

    // Redis 的 MurmurHash64A（m=0xc6a4a7935bd1e995，r=47，seed=0xadc83b19），
    // 对任意长度（含空输入）都有定义；块按 little-endian 读取，与平台字节序无关。
    private static long murmurHash64A(byte[] data) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        int len = data.length;
        long h = HASH_SEED ^ (len * m);

        int blocksEnd = len - (len & 7);
        int i = 0;
        while (i != blocksEnd) {
            long k = getLongLE(data, i);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
            i += 8;
        }

        switch (len & 7) {
            case 7:
                h ^= (long) (data[i + 6] & 0xFF) << 48;
            case 6:
                h ^= (long) (data[i + 5] & 0xFF) << 40;
            case 5:
                h ^= (long) (data[i + 4] & 0xFF) << 32;
            case 4:
                h ^= (long) (data[i + 3] & 0xFF) << 24;
            case 3:
                h ^= (long) (data[i + 2] & 0xFF) << 16;
            case 2:
                h ^= (long) (data[i + 1] & 0xFF) << 8;
            case 1:
                h ^= (long) (data[i] & 0xFF);
                h *= m;
            default:
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
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

    // Ertl "New cardinality estimation algorithms for HyperLogLog sketches"（arXiv:1702.01284）的
    // sigma/tau 辅助函数，与 Redis hllSigma/hllTau 逐行对应（双精度浮点语义一致）。
    private static double hllSigma(double x) {
        if (x == 1.0) {
            return Double.POSITIVE_INFINITY;
        }
        double zPrime;
        double y = 1.0;
        double z = x;
        do {
            x *= x;
            zPrime = z;
            z += x * y;
            y += y;
        } while (zPrime != z);
        return z;
    }

    private static double hllTau(double x) {
        if (x == 0.0 || x == 1.0) {
            return 0.0;
        }
        double zPrime;
        double y = 1.0;
        double z = 1.0 - x;
        do {
            x = Math.sqrt(x);
            zPrime = z;
            y *= 0.5;
            double d = 1.0 - x;
            z -= d * d * y;
        } while (zPrime != z);
        return z / 3.0;
    }

    // 统一 byte[] 与 BytesSlice 的 sparse 解码游标。
    private abstract static class Cursor {
        int pos;
        final int length;

        Cursor(int pos, int length) {
            this.pos = pos;
            this.length = length;
        }

        abstract byte get(int index);

        byte get() {
            return get(pos);
        }
    }

    private static final class ByteArrayCursor extends Cursor {
        private final byte[] raw;

        ByteArrayCursor(byte[] raw) {
            super(HEADER_BYTES, raw.length);
            this.raw = raw;
        }

        @Override
        byte get(int index) {
            return raw[index];
        }
    }

    private static final class SliceCursor extends Cursor {
        private final BytesSlice raw;

        SliceCursor(BytesSlice raw) {
            super(HEADER_BYTES, raw.length());
            this.raw = raw;
        }

        @Override
        byte get(int index) {
            return raw.getByte(index);
        }
    }
}
