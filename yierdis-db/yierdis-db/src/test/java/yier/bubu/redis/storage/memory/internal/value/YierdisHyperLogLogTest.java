package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

// 期望值来自真实 Redis（8.2.1）对相同 member 的响应：GET payload 字节与 PFCOUNT 计数。
public class YierdisHyperLogLogTest {
    @Test
    public void emptyMemberProducesRedisSparsePayloadAndCountsAsOne() {
        byte[] added = YierdisHyperLogLog.prepareAdd(null, List.of(new byte[0]));

        Assert.assertNotNull(added);
        // redis-cli get v0（PFADD v0 "" 之后）：register 5938 = 2。
        Assert.assertEquals(
                "48594c4c01000000000000000000008057318468cc",
                HexFormat.of().formatHex(added)
        );
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(added, registers);
        Assert.assertEquals(2, registers[5938]);
        Assert.assertEquals(1L, YierdisHyperLogLog.estimateCardinality(registers));

        // 重复添加空 member 不再改变寄存器（Redis PFADD 第二次返回 0）。
        Assert.assertNull(YierdisHyperLogLog.prepareAdd(added, List.of(new byte[0])));
    }

    @Test
    public void smallMemberSetMatchesRedisPayloadAndCount() {
        // Redis：PFADD v1 a b c 之后的 GET 字节；a=12711/2，b=15780/1，c=8436/1。
        byte[] added = YierdisHyperLogLog.prepareAdd(null, List.of(bytes("a"), bytes("b"), bytes("c")));

        Assert.assertEquals(
                "48594c4c01000000000000000000008060f38050b1844bfb80425a",
                HexFormat.of().formatHex(added)
        );
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(added, registers);
        Assert.assertEquals(2, registers[12711]);
        Assert.assertEquals(1, registers[15780]);
        Assert.assertEquals(1, registers[8436]);
        Assert.assertEquals(3L, YierdisHyperLogLog.estimateCardinality(registers));
        Assert.assertNull(YierdisHyperLogLog.prepareAdd(added, List.of(bytes("a"), bytes("b"), bytes("c"))));
    }

    @Test
    public void estimatorMatchesRedisCountsFromSparseThroughDensePromotion() {
        // Redis：member:0..999 -> PFCOUNT 1002（sparse，1898 字节）。
        byte[] sparse = YierdisHyperLogLog.prepareAdd(null, members(0, 1000));
        Assert.assertNotNull(sparse);
        Assert.assertTrue(sparse.length <= YierdisHyperLogLog.SPARSE_MAX_BYTES);
        Assert.assertEquals(1, sparse[4] & 0xFF);
        Assert.assertEquals(1898, sparse.length);
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(sparse, registers);
        Assert.assertEquals(1002L, YierdisHyperLogLog.estimateCardinality(registers));

        // Redis：member:100000..199999 -> PFCOUNT 100410（dense，12304 字节）。
        byte[] dense = YierdisHyperLogLog.prepareAdd(null, members(100000, 100000));
        Assert.assertNotNull(dense);
        Assert.assertEquals(YierdisHyperLogLog.denseLength(), dense.length);
        Assert.assertEquals(0, dense[4] & 0xFF);
        registers = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(dense, registers);
        Assert.assertEquals(100410L, YierdisHyperLogLog.estimateCardinality(registers));
    }

    @Test
    public void sparseRunEncodingUsesZeroXzeroAndValLikeRedis() {
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        registers[0] = 1;
        registers[65] = 1; // 恰好 64 个 0 的间隔：ZERO:64（单字节 0x3F）。
        registers[66] = 2;
        registers[67] = 2;
        registers[68] = 2;
        registers[69] = 2;
        registers[70] = 2; // 5 个连续的 2：VAL:2,4 + VAL:2,1。

        byte[] sparse = YierdisHyperLogLog.prepareMerge(null, false, registers);
        Assert.assertNotNull(sparse);
        Assert.assertEquals(1, sparse[4] & 0xFF);
        int[] roundTrip = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(sparse, roundTrip);
        Assert.assertArrayEquals(registers, roundTrip);

        byte[] expectedTail = new byte[]{
                (byte) 0x80, // VAL:1,1（register 0）
                0x3F, // ZERO:64（register 1..64）
                (byte) 0x80, // VAL:1,1（register 65）
                (byte) 0x87, // VAL:2,4（register 66..69）
                (byte) 0x84, // VAL:2,1（register 70）
        };
        for (int i = 0; i < expectedTail.length; i++) {
            Assert.assertEquals(expectedTail[i], sparse[YierdisHyperLogLog.HEADER_BYTES + i]);
        }
        // 尾部一个 XZERO 覆盖剩余全部寄存器。
        Assert.assertEquals(YierdisHyperLogLog.HEADER_BYTES + expectedTail.length + 2, sparse.length);
    }

    @Test
    public void registerAboveSparseRangePromotesToDense() {
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        registers[7] = 33; // 稀疏 VAL 最大只能表示 32。

        byte[] merged = YierdisHyperLogLog.prepareMerge(null, false, registers);
        Assert.assertNotNull(merged);
        Assert.assertEquals(YierdisHyperLogLog.denseLength(), merged.length);
        int[] roundTrip = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(merged, roundTrip);
        Assert.assertEquals(33, roundTrip[7]);
    }

    @Test
    public void prepareMergeFollowsRedisDestinationEncodingRules() {
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        // 全零寄存器 + 缺失 dest：Redis 创建 18 字节空 sparse（XZERO:16384，缓存置失效）。
        byte[] empty = YierdisHyperLogLog.prepareMerge(null, false, registers);
        byte[] expectedEmpty = YierdisHyperLogLog.newSparse();
        expectedEmpty[15] = (byte) 0x80;
        Assert.assertArrayEquals(expectedEmpty, empty);

        byte[] sparse = YierdisHyperLogLog.prepareAdd(null, List.of(bytes("a"), bytes("b")));
        Assert.assertNotNull(sparse);
        int[] sparseRegisters = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(sparse, sparseRegisters);

        // 同寄存器同编码：无需重写。
        Assert.assertNull(YierdisHyperLogLog.prepareMerge(sparse, false, sparseRegisters));
        // source 有 dense：即使寄存器一致也升格 dense（Redis use_dense）。
        byte[] promoted = YierdisHyperLogLog.prepareMerge(sparse, true, sparseRegisters);
        Assert.assertNotNull(promoted);
        Assert.assertEquals(YierdisHyperLogLog.denseLength(), promoted.length);
        // dest 已是 dense：永不降级。
        Assert.assertNull(YierdisHyperLogLog.prepareMerge(promoted, false, sparseRegisters));
    }

    @Test
    public void invalidPayloadsRaiseRedisWrongTypeText() {
        // 旧 Yierdis 私有 "HLL1" payload 不再可读（内存态直接切换，无迁移）。
        byte[] legacy = new byte[YierdisHyperLogLog.HEADER_BYTES];
        legacy[0] = 'H';
        legacy[1] = 'L';
        legacy[2] = 'L';
        legacy[3] = '1';
        legacy[4] = 0; // 旧格式的 sparse encoding 字节
        assertInvalidHll(legacy);

        assertInvalidHll(new byte[0]);
        assertInvalidHll(bytes("not-an-hll"));
    }

    @Test
    public void invalidDenseLengthAndUnknownEncodingRaiseWrongType() {
        byte[] badEncoding = YierdisHyperLogLog.newSparse();
        badEncoding[4] = 7;
        assertInvalidHll(badEncoding);

        byte[] denseWrongLength = new byte[YierdisHyperLogLog.denseLength() + 1];
        byte[] header = YierdisHyperLogLog.newDenseEmpty();
        System.arraycopy(header, 0, denseWrongLength, 0, header.length);
        assertInvalidHll(denseWrongLength);
    }

    @Test
    public void corruptSparseContentRaisesWrongType() {
        // header 合法但游程没有精确覆盖 16384 个寄存器。
        byte[] truncated = YierdisHyperLogLog.newSparse();
        truncated = java.util.Arrays.copyOf(truncated, truncated.length - 1);
        assertInvalidHll(truncated);

        byte[] overflow = new byte[YierdisHyperLogLog.HEADER_BYTES + 2];
        System.arraycopy(YierdisHyperLogLog.newSparse(), 0, overflow, 0, YierdisHyperLogLog.HEADER_BYTES);
        // XZERO:16384 覆盖全部寄存器后不能再有内容；这里手工构造 XZERO:16384 + VAL:1,1 之外的越界：
        overflow[YierdisHyperLogLog.HEADER_BYTES] = 0x7F; // XZERO 高字节
        overflow[YierdisHyperLogLog.HEADER_BYTES + 1] = (byte) 0xFF; // XZERO:16384
        // 长度到这里恰好合法；追加一个 VAL 使总数溢出。
        overflow = java.util.Arrays.copyOf(overflow, overflow.length + 1);
        overflow[overflow.length - 1] = (byte) 0x80;
        assertInvalidHll(overflow);
    }

    @Test
    public void denseRegistersClampAndRoundTrip() {
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        registers[0] = -1;
        registers[1] = 1;
        registers[2] = 100;

        byte[] dense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
        int[] merged = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(dense, merged);

        Assert.assertEquals(0, merged[0]);
        Assert.assertEquals(1, merged[1]);
        Assert.assertEquals(63, merged[2]);
        Assert.assertEquals(YierdisHyperLogLog.denseLength(), dense.length);
    }

    private static List<byte[]> members(int startInclusive, int count) {
        List<byte[]> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(bytes("member:" + (startInclusive + i)));
        }
        return out;
    }

    private static void assertInvalidHll(byte[] raw) {
        try {
            YierdisHyperLogLog.mergeHllIntoRegisters(raw, new int[YierdisHyperLogLog.REGISTERS]);
            Assert.fail("expected invalid HyperLogLog error");
        } catch (YierdisCommandException expected) {
            Assert.assertEquals(YierdisHyperLogLog.INVALID_HLL_ERROR, expected.getMessage());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
