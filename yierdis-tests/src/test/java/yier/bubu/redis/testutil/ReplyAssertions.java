package yier.bubu.redis.testutil;

import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class ReplyAssertions {
    private ReplyAssertions() {
    }

    public static ReplySimpleString assertSimpleString(String expected, ReplyObject reply) {
        Assert.assertTrue("expected simple string reply but got " + typeName(reply), reply instanceof ReplySimpleString);
        ReplySimpleString actual = (ReplySimpleString) reply;
        Assert.assertEquals(expected, actual.value());
        return actual;
    }

    public static ReplyBulkString assertBulkString(String expected, ReplyObject reply) {
        return assertBulkString(expected.getBytes(StandardCharsets.UTF_8), reply);
    }

    public static ReplyBulkString assertBulkString(byte[] expected, ReplyObject reply) {
        Assert.assertTrue("expected bulk string reply but got " + typeName(reply), reply instanceof ReplyBulkString);
        ReplyBulkString actual = (ReplyBulkString) reply;
        Assert.assertTrue(
                "expected bulk bytes " + Arrays.toString(expected) + " but got " + Arrays.toString(actual.data()),
                Arrays.equals(expected, actual.data())
        );
        return actual;
    }

    public static ReplyInteger assertInteger(long expected, ReplyObject reply) {
        Assert.assertTrue("expected integer reply but got " + typeName(reply), reply instanceof ReplyInteger);
        ReplyInteger actual = (ReplyInteger) reply;
        Assert.assertEquals(expected, actual.value());
        return actual;
    }

    public static ReplyNull assertNull(ReplyObject reply) {
        Assert.assertTrue("expected null reply but got " + typeName(reply), reply instanceof ReplyNull);
        return (ReplyNull) reply;
    }

    public static ReplyError assertErrorContaining(String expectedFragment, ReplyObject reply) {
        Assert.assertTrue("expected error reply but got " + typeName(reply), reply instanceof ReplyError);
        ReplyError actual = (ReplyError) reply;
        Assert.assertTrue(
                "expected error containing " + expectedFragment + " but got " + actual.message(),
                actual.message().contains(expectedFragment)
        );
        return actual;
    }

    public static ReplyArray assertArraySize(int expectedSize, ReplyObject reply) {
        Assert.assertTrue("expected array reply but got " + typeName(reply), reply instanceof ReplyArray);
        ReplyArray actual = (ReplyArray) reply;
        Assert.assertEquals(expectedSize, actual.values().size());
        return actual;
    }

    private static String typeName(ReplyObject reply) {
        return reply == null ? "null" : reply.getClass().getSimpleName();
    }
}
