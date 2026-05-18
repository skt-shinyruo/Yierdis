package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

public class YierdisBenchServerArgsTest {
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
