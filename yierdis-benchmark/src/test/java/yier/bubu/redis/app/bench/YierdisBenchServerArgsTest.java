package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

public class YierdisBenchServerArgsTest {
    @Test
    public void nativeSlotCapacityRoundTripsThroughNormalizeCopyAndArgv() {
        YierdisBenchServerArgs args = parse(
                "--databases", "1",
                "--nativeSlotCapacity", "2097152"
        );

        args.normalizeAndValidate();

        Assert.assertEquals(1, args.databases);
        Assert.assertEquals(2_097_152, args.nativeSlotCapacity);
        Assert.assertEquals(args.toArgv(), args.copy().toArgv());
        Assert.assertTrue(args.toArgv().contains("--nativeSlotCapacity"));
    }

    @Test
    public void nativeSlotCapacityAllowsZeroAsDefaultSentinelAndRejectsNegativeValues() {
        YierdisBenchServerArgs zero = parse("--nativeSlotCapacity", "0");
        zero.normalizeAndValidate();
        Assert.assertEquals(0, zero.nativeSlotCapacity);

        YierdisBenchServerArgs negative = parse("--nativeSlotCapacity", "-1");
        assertThrows(IllegalArgumentException.class, negative::normalizeAndValidate);
    }

    @Test
    public void protocolLimitsRejectValuesAboveDecoderSafeMaximum() {
        YierdisBenchServerArgs bulkArgs = parse("--protocolMaxBulkBytes", Integer.toString(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, bulkArgs::normalizeAndValidate);

        YierdisBenchServerArgs argcArgs = parse("--protocolMaxArgs", Integer.toString(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, argcArgs::normalizeAndValidate);
    }

    private static YierdisBenchServerArgs parse(String... argv) {
        YierdisBenchServerArgs args = new YierdisBenchServerArgs();
        new CommandLine(args).parseArgs(argv);
        return args;
    }

    private static void assertThrows(Class<? extends Throwable> expected, Runnable r) {
        try {
            r.run();
            Assert.fail("expected exception: " + expected.getSimpleName());
        } catch (Throwable t) {
            if (!expected.isInstance(t)) {
                Assert.fail("expected " + expected.getSimpleName() + ", got: " + t.getClass().getName());
            }
        }
    }
}
