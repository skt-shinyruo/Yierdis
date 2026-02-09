package yier.bubu.redis.args;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.protocol.ProtocolLimits;

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
        YierdisServerArgs args = parse("--port", "-1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidWatermarkOrderIsRejected() {
        YierdisServerArgs args = parse("--backpressureHigh", "10", "--backpressureLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidBytesWatermarkOrderIsRejected() {
        YierdisServerArgs args = parse("--backpressureBytesHigh", "10", "--backpressureBytesLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void bytesLowWithoutBytesHighIsRejected() {
        YierdisServerArgs args = parse("--backpressureBytesHigh", "0", "--backpressureBytesLow", "1");
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

    @Test
    public void protocolDefaultsMatchProtocolLimitsSsot() {
        YierdisServerArgs args = new YierdisServerArgs();
        Assert.assertEquals(ProtocolLimits.DEFAULT_MAX_REQUEST_PAYLOAD_BYTES, args.protocolMaxBulkBytes);
        Assert.assertEquals(ProtocolLimits.DEFAULT_MAX_ARGS, args.protocolMaxArgs);
        Assert.assertEquals(ProtocolLimits.DEFAULT_MAX_HEADER_BYTES, args.protocolMaxLineBytes);
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
