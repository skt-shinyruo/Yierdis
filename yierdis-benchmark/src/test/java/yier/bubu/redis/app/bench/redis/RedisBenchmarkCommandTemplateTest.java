package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedisBenchmarkCommandTemplateTest {
    private static final long TWELVE_DIGIT_LIMIT = 1_000_000_000_000L;
    private final RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();

    @Test
    public void inlinePingUsesOfficialBytes() {
        RedisBenchmarkCase ping = catalog.caseById("ping_inline");
        PreparedPipeline pipeline = ping.template()
                .prepare(1, new byte[]{'x'}, OptionalLong.empty());

        Assert.assertArrayEquals(
                ascii("PING\r\n"),
                pipeline.bytesForWrite(new BenchmarkRandom(1L))
        );
    }

    @Test
    public void omittedKeyspaceKeepsLiteralPlaceholder() {
        PreparedPipeline pipeline = catalog.caseById("set").template()
                .prepare(1, new byte[]{'a', 'b', 'c'}, OptionalLong.empty());

        String wire = wire(pipeline.bytesForWrite(new BenchmarkRandom(1L)));

        Assert.assertTrue(wire.contains("key:__rand_int__"));
    }

    @Test
    public void explicitZeroAndPositiveKeyspaceRenderTwelveDigitsPerOccurrence() {
        RedisBenchmarkCommandTemplate mset = catalog.caseById("mset").template();
        PreparedPipeline zero = mset.prepare(1, new byte[]{'x'}, OptionalLong.of(0));
        String zeroWire = wire(zero.bytesForWrite(new BenchmarkRandom(1L)));
        Assert.assertEquals(10, occurrences(zeroWire, "key:000000000000"));
        Assert.assertFalse(zeroWire.contains("__rand_int__"));

        PreparedPipeline random = mset.prepare(2, new byte[]{'x'}, OptionalLong.of(10_000));
        String first = wire(random.bytesForWrite(new BenchmarkRandom(7L)));
        Assert.assertFalse(first.contains("__rand_int__"));
        Assert.assertEquals(20, occurrences(first, "key:"));
        Matcher matcher = Pattern.compile("key:(\\d{12})").matcher(first);
        Set<String> renderedKeys = new HashSet<>();
        while (matcher.find()) {
            renderedKeys.add(matcher.group(1));
        }
        Assert.assertTrue("each marker must consume the random stream", renderedKeys.size() > 1);
    }

    @Test
    public void randomizedZaddUsesIndependentTwelveDigitScoreAndMember() {
        PreparedPipeline zadd = catalog.caseById("zadd").template()
                .prepare(1, new byte[]{'x'}, OptionalLong.of(1_000_000));

        String wire = wire(zadd.bytesForWrite(new BenchmarkRandom(11L)));
        Matcher matcher = Pattern.compile("(?:\\r\\n|element:)(\\d{12})\\r\\n").matcher(wire);
        List<String> randomArguments = new ArrayList<>();
        while (matcher.find()) {
            randomArguments.add(matcher.group(1));
        }

        Assert.assertEquals(2, randomArguments.size());
        Assert.assertNotEquals(randomArguments.get(0), randomArguments.get(1));
    }

    @Test
    public void officialPayloadGeneratorIsDeterministicAndMatchesRecurrence() {
        Assert.assertArrayEquals(BenchmarkPayload.generate(32), BenchmarkPayload.generate(32));
        Assert.assertEquals(3, BenchmarkPayload.generate(3).length);
        Assert.assertArrayEquals(ascii("VXKeHogKgJ=[5V9_"), BenchmarkPayload.generate(16));
        Assert.assertArrayEquals(new byte[0], BenchmarkPayload.generate(0));
    }

    @Test
    public void fixedModeWireMatchesEveryOfficialRespTemplate() {
        assertFixedResp("ping_mbulk", "PING");
        assertFixedResp("set", "SET", "key:__rand_int__", "abc");
        assertFixedResp("get", "GET", "key:__rand_int__");
        assertFixedResp("incr", "INCR", "counter:__rand_int__");
        assertFixedResp("lpush", "LPUSH", "mylist", "abc");
        assertFixedResp("rpush", "RPUSH", "mylist", "abc");
        assertFixedResp("lpop", "LPOP", "mylist");
        assertFixedResp("rpop", "RPOP", "mylist");
        assertFixedResp("sadd", "SADD", "myset", "element:__rand_int__");
        assertFixedResp("hset", "HSET", "myhash", "element:__rand_int__", "abc");
        assertFixedResp("spop", "SPOP", "myset");
        assertFixedResp("zadd", "ZADD", "myzset", "0", "element:__rand_int__");
        assertFixedResp("zpopmin", "ZPOPMIN", "myzset");
        assertFixedResp("lrange_setup", "LPUSH", "mylist", "abc");
        assertFixedResp("lrange_100", "LRANGE", "mylist", "0", "99");
        assertFixedResp("lrange_300", "LRANGE", "mylist", "0", "299");
        assertFixedResp("lrange_500", "LRANGE", "mylist", "0", "499");
        assertFixedResp("lrange_600", "LRANGE", "mylist", "0", "599");
        assertFixedResp("mset", msetArguments());
        assertFixedResp("xadd", "XADD", "mystream", "*", "myfield", "abc");
    }

    @Test
    public void pipelineConcatenatesCompleteInlineAndRespFrames() {
        byte[] inlineFrame = ascii("PING\r\n");
        PreparedPipeline inline = catalog.caseById("ping_inline").template()
                .prepare(3, ascii("abc"), OptionalLong.empty());
        Assert.assertArrayEquals(
                repeated(inlineFrame, 3),
                inline.bytesForWrite(new BenchmarkRandom(1L))
        );

        byte[] respFrame = RespClientCodec.encodeCommand(List.of(
                ascii("GET"), ascii("key:__rand_int__")
        ));
        PreparedPipeline resp = catalog.caseById("get").template()
                .prepare(3, ascii("abc"), OptionalLong.empty());
        Assert.assertArrayEquals(
                repeated(respFrame, 3),
                resp.bytesForWrite(new BenchmarkRandom(1L))
        );
    }

    @Test
    public void everyPipelineMarkerConsumesStreamAndEveryWriteRerandomizesAllOffsets() {
        long bound = 10_000;
        PreparedPipeline mset = catalog.caseById("mset").template()
                .prepare(2, ascii("abc"), OptionalLong.of(bound));
        BenchmarkRandom actual = new BenchmarkRandom(91L);
        BenchmarkRandom expected = new BenchmarkRandom(91L);

        String first = wire(mset.bytesForWrite(actual));
        Assert.assertEquals(expectedDigits(expected, bound, 20), renderedKeys(first));

        String second = wire(mset.bytesForWrite(actual));
        Assert.assertEquals(expectedDigits(expected, bound, 20), renderedKeys(second));
        Assert.assertNotEquals(first, second);
    }

    @Test
    public void fixedModeLeavesZaddScoreAtAsciiZeroAndConsumesNoRandomValues() {
        PreparedPipeline zadd = catalog.caseById("zadd").template()
                .prepare(1, ascii("abc"), OptionalLong.empty());
        BenchmarkRandom actual = new BenchmarkRandom(37L);

        byte[] first = zadd.bytesForWrite(actual).clone();
        byte[] second = zadd.bytesForWrite(actual).clone();

        Assert.assertArrayEquals(RespClientCodec.encodeCommand(List.of(
                ascii("ZADD"), ascii("myzset"), ascii("0"), ascii("element:__rand_int__")
        )), first);
        Assert.assertArrayEquals(first, second);
        BenchmarkRandom expected = new BenchmarkRandom(37L);
        Assert.assertEquals(expected.nextLong(1_000_000), actual.nextLong(1_000_000));
    }

    @Test
    public void copyForClientOwnsIndependentMutableBytesAndOffsets() throws Exception {
        PreparedPipeline source = catalog.caseById("set").template()
                .prepare(1, ascii("abc"), OptionalLong.of(1_000_000));
        PreparedPipeline firstClient = source.copyForClient();
        PreparedPipeline sibling = source.copyForClient();

        byte[] sourceBytes = source.bytesForWrite(new BenchmarkRandom(3L));
        byte[] sourceSnapshot = sourceBytes.clone();
        byte[] siblingBytes = sibling.bytesForWrite(new BenchmarkRandom(5L));
        byte[] siblingSnapshot = siblingBytes.clone();
        byte[] firstClientBytes = firstClient.bytesForWrite(new BenchmarkRandom(7L));
        firstClient.bytesForWrite(new BenchmarkRandom(11L));

        Assert.assertNotSame(sourceBytes, siblingBytes);
        Assert.assertNotSame(sourceBytes, firstClientBytes);
        Assert.assertNotSame(siblingBytes, firstClientBytes);
        Assert.assertArrayEquals(sourceSnapshot, sourceBytes);
        Assert.assertArrayEquals(siblingSnapshot, siblingBytes);

        Field offsets = Arrays.stream(PreparedPipeline.class.getDeclaredFields())
                .filter(field -> field.getType() == int[].class)
                .findFirst()
                .orElseThrow();
        offsets.setAccessible(true);
        Assert.assertNotSame(offsets.get(source), offsets.get(firstClient));
        Assert.assertNotSame(offsets.get(source), offsets.get(sibling));
    }

    @Test
    public void preparedPipelineKeepsMutableBytesOutOfPublicApi() throws Exception {
        Assert.assertFalse(Modifier.isPublic(PreparedPipeline.class.getModifiers()));
        Assert.assertTrue(Modifier.isFinal(PreparedPipeline.class.getModifiers()));

        for (Field field : PreparedPipeline.class.getDeclaredFields()) {
            Assert.assertTrue(field.getName(), Modifier.isPrivate(field.getModifiers()));
        }
        for (Method method : PreparedPipeline.class.getDeclaredMethods()) {
            if (method.getReturnType() == byte[].class) {
                Assert.assertFalse(method.getName(), Modifier.isPublic(method.getModifiers()));
            }
        }

        Method bytesForWrite = PreparedPipeline.class
                .getDeclaredMethod("bytesForWrite", BenchmarkRandom.class);
        Assert.assertFalse(Modifier.isPublic(bytesForWrite.getModifiers()));
    }

    @Test
    public void invalidPreparationAndGenerationInputsAreRejectedClearly() {
        RedisBenchmarkCommandTemplate set = catalog.caseById("set").template();

        assertMessage(IllegalArgumentException.class, "pipeline",
                () -> set.prepare(0, ascii("abc"), OptionalLong.empty()));
        assertMessage(IllegalArgumentException.class, "pipeline",
                () -> set.prepare(-1, ascii("abc"), OptionalLong.empty()));
        assertMessage(NullPointerException.class, "payload",
                () -> set.prepare(1, null, OptionalLong.empty()));
        assertMessage(NullPointerException.class, "keyspace",
                () -> set.prepare(1, ascii("abc"), null));
        assertMessage(IllegalArgumentException.class, "keyspace",
                () -> set.prepare(1, ascii("abc"), OptionalLong.of(-1)));
        assertMessage(IllegalArgumentException.class, "12 digits",
                () -> set.prepare(1, ascii("abc"), OptionalLong.of(TWELVE_DIGIT_LIMIT + 1)));

        PreparedPipeline fixed = set.prepare(1, ascii("abc"), OptionalLong.empty());
        assertMessage(NullPointerException.class, "random", () -> fixed.bytesForWrite(null));
        assertMessage(IllegalArgumentException.class, "size", () -> BenchmarkPayload.generate(-1));
    }

    @Test
    public void benchmarkRandomIsSeededBoundedAndRejectsNonpositiveBounds() {
        BenchmarkRandom first = new BenchmarkRandom(123L);
        BenchmarkRandom second = new BenchmarkRandom(123L);

        for (int index = 0; index < 100; index++) {
            long firstValue = first.nextLong(17);
            Assert.assertEquals(firstValue, second.nextLong(17));
            Assert.assertTrue(firstValue >= 0);
            Assert.assertTrue(firstValue < 17);
        }

        assertMessage(IllegalArgumentException.class, "bound", () -> first.nextLong(0));
        assertMessage(IllegalArgumentException.class, "bound", () -> first.nextLong(-1));
    }

    @Test
    public void writeTwelveDigitsZeroPadsWithoutTouchingNeighborsOrTruncating() {
        byte[] representative = ascii("xxxxxxxxxxxxxxxx");
        BenchmarkRandom.writeTwelveDigits(representative, 2, 42);
        Assert.assertArrayEquals(ascii("xx000000000042xx"), representative);

        byte[] maximum = ascii("xxxxxxxxxxxxxx");
        BenchmarkRandom.writeTwelveDigits(maximum, 1, TWELVE_DIGIT_LIMIT - 1);
        Assert.assertArrayEquals(ascii("x999999999999x"), maximum);

        assertMessage(NullPointerException.class, "target",
                () -> BenchmarkRandom.writeTwelveDigits(null, 0, 0));
        assertMessage(IndexOutOfBoundsException.class, "offset",
                () -> BenchmarkRandom.writeTwelveDigits(new byte[12], -1, 0));
        assertMessage(IndexOutOfBoundsException.class, "offset",
                () -> BenchmarkRandom.writeTwelveDigits(new byte[12], 1, 0));
        assertMessage(IllegalArgumentException.class, "value",
                () -> BenchmarkRandom.writeTwelveDigits(new byte[12], 0, -1));
        assertMessage(IllegalArgumentException.class, "12 digits",
                () -> BenchmarkRandom.writeTwelveDigits(
                        new byte[12], 0, TWELVE_DIGIT_LIMIT
                ));
    }

    private void assertFixedResp(String id, String... arguments) {
        byte[] expected = RespClientCodec.encodeCommand(Arrays.stream(arguments)
                .map(RedisBenchmarkCommandTemplateTest::ascii)
                .toList());
        PreparedPipeline prepared = catalog.caseById(id).template()
                .prepare(1, ascii("abc"), OptionalLong.empty());
        Assert.assertArrayEquals(
                id,
                expected,
                prepared.bytesForWrite(new BenchmarkRandom(1L))
        );
    }

    private static String[] msetArguments() {
        String[] arguments = new String[21];
        arguments[0] = "MSET";
        for (int pair = 0; pair < 10; pair++) {
            arguments[pair * 2 + 1] = "key:__rand_int__";
            arguments[pair * 2 + 2] = "abc";
        }
        return arguments;
    }

    private static List<String> expectedDigits(BenchmarkRandom random, long bound, int count) {
        List<String> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(String.format(Locale.ROOT, "%012d", random.nextLong(bound)));
        }
        return values;
    }

    private static List<String> renderedKeys(String wire) {
        Matcher matcher = Pattern.compile("key:(\\d{12})").matcher(wire);
        List<String> values = new ArrayList<>();
        while (matcher.find()) {
            values.add(matcher.group(1));
        }
        return values;
    }

    private static byte[] repeated(byte[] frame, int count) {
        byte[] repeated = new byte[frame.length * count];
        for (int index = 0; index < count; index++) {
            System.arraycopy(frame, 0, repeated, frame.length * index, frame.length);
        }
        return repeated;
    }

    private static int occurrences(String value, String target) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(target, offset)) >= 0) {
            count++;
            offset += target.length();
        }
        return count;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static String wire(byte[] value) {
        return new String(value, StandardCharsets.US_ASCII);
    }

    private static <T extends Throwable> void assertMessage(
            Class<T> type,
            String expectedText,
            ThrowingRunnable action
    ) {
        T failure = Assert.assertThrows(type, action);
        Assert.assertTrue(
                "expected message containing '" + expectedText + "' but was '" + failure.getMessage() + "'",
                failure.getMessage() != null && failure.getMessage().contains(expectedText)
        );
    }
}
