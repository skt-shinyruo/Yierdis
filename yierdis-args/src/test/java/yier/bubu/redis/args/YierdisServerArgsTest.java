package yier.bubu.redis.args;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

public class YierdisServerArgsTest {
    @Test
    public void helpParses() {
        YierdisServerArgs args = new YierdisServerArgs();
        new CommandLine(args).parseArgs("--help");
        Assert.assertTrue(args.help);
    }

    @Test
    public void normalizeLowercasesBackendAndPolicy() {
        YierdisServerArgs args = parse("--offheapBackend", "UNSAFE", "--maxmemoryPolicy", "ALLKEYS-LRU");
        args.normalizeAndValidate();
        Assert.assertEquals("unsafe", args.offheapBackend);
        Assert.assertEquals("allkeys-lru", args.maxmemoryPolicy);
    }

    @Test
    public void invalidPortIsRejected() {
        YierdisServerArgs args = parse("--port", "0");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidWatermarkOrderIsRejected() {
        YierdisServerArgs args = parse("--backpressureHigh", "10", "--backpressureLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidOffheapBackendIsRejected() {
        YierdisServerArgs args = parse("--offheapBackend", "???");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void offheapBackendNoneWithNonzeroMaxBytesIsRejected() {
        YierdisServerArgs args = parse("--offheapBackend", "none", "--offheapMaxBytes", "1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidMaxmemoryPolicyIsRejected() {
        YierdisServerArgs args = parse("--maxmemoryPolicy", "random-evict");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    private static YierdisServerArgs parse(String... argv) {
        YierdisServerArgs args = new YierdisServerArgs();
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

