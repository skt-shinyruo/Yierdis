package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class YierdisHyperLogLogTest {
    @Test
    public void sparseHllAddsElementsAndMergesIntoRegisters() {
        try (TestBackend runtime = TestBackend.open("hll-sparse");
             StringRoot root = new StringRoot(runtime.backend())) {
            ValueHandle handle = root.store(YierdisHyperLogLog.newSparse());

            Assert.assertTrue(YierdisHyperLogLog.isHllString(root, handle));
            Assert.assertFalse(YierdisHyperLogLog.isDense(root, handle));
            Assert.assertTrue(YierdisHyperLogLog.pfAdd(root, handle, List.of(bytes("a"), bytes("b"), bytes("c"))));
            Assert.assertFalse(YierdisHyperLogLog.isDense(root, handle));
            Assert.assertFalse(YierdisHyperLogLog.pfAdd(root, handle, List.of(bytes("a"), bytes("b"), bytes("c"))));

            int[] registers = new int[YierdisHyperLogLog.REGISTERS];
            YierdisHyperLogLog.mergeHllIntoRegisters(root.copy(handle), registers);

            Assert.assertTrue(YierdisHyperLogLog.estimateCardinality(registers) >= 3L);
            root.release(handle);
        }
    }

    @Test
    public void denseHllUpdatesInPlaceAndMergesViaBytesSlice() {
        try (TestBackend runtime = TestBackend.open("hll-dense");
             StringRoot root = new StringRoot(runtime.backend())) {
            ValueHandle handle = root.store(YierdisHyperLogLog.newDenseEmpty());
            long estimatedBytes = root.estimatedBytes(handle);

            Assert.assertTrue(YierdisHyperLogLog.isDense(root, handle));
            Assert.assertTrue(YierdisHyperLogLog.pfAdd(root, handle, List.of(bytes("dense-a"), bytes("dense-b"))));
            Assert.assertEquals(estimatedBytes, root.estimatedBytes(handle));

            int[] registers = new int[YierdisHyperLogLog.REGISTERS];
            YierdisHyperLogLog.mergeHllIntoRegisters(root.slice(handle), registers);

            Assert.assertTrue(YierdisHyperLogLog.estimateCardinality(registers) >= 2L);
            root.release(handle);
        }
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
