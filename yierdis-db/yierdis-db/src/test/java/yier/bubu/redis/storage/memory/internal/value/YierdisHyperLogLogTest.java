package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class YierdisHyperLogLogTest {
    @Test
    public void sparseHllAddsElementsAndMergesIntoRegisters() {
        List<byte[]> elements = List.of(bytes("a"), bytes("b"), bytes("c"));
        byte[] updated = YierdisHyperLogLog.prepareAdd(YierdisHyperLogLog.newSparse(), elements);

        Assert.assertNotNull(updated);
        Assert.assertNull(YierdisHyperLogLog.prepareAdd(updated, elements));
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(updated, registers);
        Assert.assertTrue(YierdisHyperLogLog.estimateCardinality(registers) >= 3L);
    }

    @Test
    public void denseHllFeedsTheMutationPath() {
        byte[] updated = YierdisHyperLogLog.prepareAdd(
                YierdisHyperLogLog.newDenseEmpty(),
                List.of(bytes("dense-a"), bytes("dense-b"))
        );

        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        YierdisHyperLogLog.mergeHllIntoRegisters(updated, registers);
        Assert.assertTrue(YierdisHyperLogLog.estimateCardinality(registers) >= 2L);
    }

    @Test
    public void denseBytesFromRegistersClampsAndRoundTripsThroughMerge() {
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

    @Test
    public void sparseLengthUpperBoundIgnoresNullAndEmptyElementsAndCapsAtDenseLength() {
        List<byte[]> elements = new ArrayList<>();
        elements.add(null);
        elements.add(new byte[0]);
        elements.add(bytes("x"));

        Assert.assertEquals(YierdisHyperLogLog.HEADER_BYTES + 3, YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements));

        for (int i = 0; i < YierdisHyperLogLog.REGISTERS; i++) {
            elements.add(bytes("e" + i));
        }
        Assert.assertEquals(YierdisHyperLogLog.denseLength(), YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
