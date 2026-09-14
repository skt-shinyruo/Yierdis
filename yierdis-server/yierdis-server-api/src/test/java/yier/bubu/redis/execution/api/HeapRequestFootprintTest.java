package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class HeapRequestFootprintTest {
    @Test
    public void estimateCoversRequestObjectOuterArraySlotsAndArgumentHeaders() {
        Assert.assertEquals(48L, HeapRequestFootprint.estimateBytes(new byte[0][]));
        Assert.assertEquals(80L, HeapRequestFootprint.estimateBytes(new byte[][]{ascii("PING")}));
        Assert.assertEquals(112L, HeapRequestFootprint.estimateBytes(new byte[][]{ascii("SET"), ascii("key")}));

        Assert.assertEquals(48, HeapRequestFootprint.estimateRetainedBytes(new byte[0][]));
        Assert.assertEquals(112, HeapRequestFootprint.estimateRetainedBytes(new byte[][]{ascii("SET"), ascii("key")}));
    }

    @Test
    public void nullArgumentKeepsReferenceSlotButSkipsArrayOverhead() {
        Assert.assertEquals(88, HeapRequestFootprint.estimateRetainedBytes(new byte[][]{ascii("ECHO"), null}));
    }

    @Test
    public void samePayloadWithMoreArgumentsRetainsMoreBytes() {
        byte[][] singleArgument = new byte[][]{ascii("ab")};
        byte[][] splitArguments = new byte[][]{ascii("a"), ascii("b")};

        Assert.assertEquals(80, HeapRequestFootprint.estimateRetainedBytes(singleArgument));
        Assert.assertEquals(112, HeapRequestFootprint.estimateRetainedBytes(splitArguments));
        Assert.assertTrue(
                HeapRequestFootprint.estimateRetainedBytes(splitArguments)
                        > HeapRequestFootprint.estimateRetainedBytes(singleArgument)
        );
    }

    @Test
    public void argumentEstimateAlignsPayloadToEightBytes() {
        Assert.assertEquals(16, HeapRequestFootprint.argumentRetainedBytes(0));
        Assert.assertEquals(24, HeapRequestFootprint.argumentRetainedBytes(8));
        Assert.assertEquals(32, HeapRequestFootprint.argumentRetainedBytes(9));
    }

    @Test
    public void incrementalAccumulationMatchesWholeArgvEstimate() {
        byte[][] argv = new byte[][]{ascii("SET"), null, ascii("value")};

        int incremental = HeapRequestFootprint.baseRetainedBytes(argv.length);
        incremental = HeapRequestFootprint.addRetainedBytes(
                incremental, HeapRequestFootprint.argumentRetainedBytes(3));
        incremental = HeapRequestFootprint.addRetainedBytes(
                incremental, HeapRequestFootprint.argumentRetainedBytes(5));

        Assert.assertEquals(HeapRequestFootprint.estimateRetainedBytes(argv), incremental);
        Assert.assertEquals(120, incremental);
    }

    @Test
    public void estimatesSaturateInsteadOfOverflowing() {
        Assert.assertEquals(Integer.MAX_VALUE, HeapRequestFootprint.argumentRetainedBytes(Integer.MAX_VALUE));
        Assert.assertEquals(Integer.MAX_VALUE, HeapRequestFootprint.baseRetainedBytes(Integer.MAX_VALUE));
        Assert.assertEquals(Integer.MAX_VALUE, HeapRequestFootprint.addRetainedBytes(Integer.MAX_VALUE - 1, 16));
    }

    @Test
    public void negativeInputsAreClampedToZero() {
        Assert.assertEquals(48, HeapRequestFootprint.baseRetainedBytes(-4));
        Assert.assertEquals(16, HeapRequestFootprint.argumentRetainedBytes(-3));
        Assert.assertEquals(3, HeapRequestFootprint.addRetainedBytes(-4, 3));
    }

    @Test
    public void longDomainAdmissionChargesSumToTheWholeArgvEstimate() {
        Assert.assertEquals(32L, HeapRequestFootprint.requestFixedBytes());
        Assert.assertEquals(16L, HeapRequestFootprint.outerArgvBytes(0));
        Assert.assertEquals(96L, HeapRequestFootprint.outerArgvBytes(10));
        Assert.assertEquals(16L, HeapRequestFootprint.argumentBytes(0));
        Assert.assertEquals(24L, HeapRequestFootprint.argumentBytes(3));
        Assert.assertEquals(32L, HeapRequestFootprint.argumentBytes(9));

        byte[][] argv = new byte[][]{ascii("SET"), null, ascii("value")};
        long sum = HeapRequestFootprint.requestFixedBytes()
                + HeapRequestFootprint.outerArgvBytes(argv.length)
                + HeapRequestFootprint.argumentBytes(3)
                + HeapRequestFootprint.argumentBytes(5);
        Assert.assertEquals(HeapRequestFootprint.estimateBytes(argv), sum);
        Assert.assertEquals(120L, sum);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
