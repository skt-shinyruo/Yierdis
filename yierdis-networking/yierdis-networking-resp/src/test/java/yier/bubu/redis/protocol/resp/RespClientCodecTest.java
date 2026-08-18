package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class RespClientCodecTest {
    @Test
    public void encodesCommand() throws Exception {
        Assert.assertEquals("*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n",
                ascii(RespClientCodec.encodeCommand(List.of(bytes("PING"), bytes("hey")))));
    }

    @Test
    public void readsSimpleBulkIntegerNullAndArrayReplies() throws Exception {
        RespClientCodec.RespReply simple = RespClientCodec.readReply(in("+OK\r\n"), 1024);
        Assert.assertEquals(RespClientCodec.RespReply.Kind.SIMPLE_STRING, simple.kind());
        Assert.assertEquals("OK", simple.text());
        Assert.assertArrayEquals(bytes("abc"), RespClientCodec.readReply(in("$3\r\nabc\r\n"), 1024).bytes());
        Assert.assertEquals(Long.valueOf(7), RespClientCodec.readReply(in(":7\r\n"), 1024).integer());
        Assert.assertTrue(RespClientCodec.readReply(in("$-1\r\n"), 1024).isNull());
        Assert.assertEquals(2, RespClientCodec.readReply(in("*2\r\n+OK\r\n:1\r\n"), 1024).values().size());
    }

    @Test
    public void encodesEmptyCommandAndNullArgumentsAsEmptyBulkStrings() throws Exception {
        Assert.assertEquals("*0\r\n", ascii(RespClientCodec.encodeCommand(List.of())));

        List<byte[]> args = Arrays.asList(null, new byte[0], bytes("x"));
        String expected = "*3\r\n$0\r\n\r\n$0\r\n\r\n$1\r\nx\r\n";
        Assert.assertEquals(expected, ascii(RespClientCodec.encodeCommand(args)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespClientCodec.writeCommand(out, args);
        Assert.assertEquals(expected, out.toString(StandardCharsets.US_ASCII));
        Assert.assertThrows(NullPointerException.class, () -> RespClientCodec.writeCommand(null, args));
        Assert.assertThrows(NullPointerException.class, () -> RespClientCodec.writeCommand(out, null));
    }

    @Test
    public void readsErrorsResp3NullNullArraysAndNestedReplies() throws Exception {
        RespClientCodec.RespReply error = RespClientCodec.readReply(in("-ERR failure\r\n"), 1024);
        Assert.assertEquals(RespClientCodec.RespReply.Kind.ERROR, error.kind());
        Assert.assertEquals("ERR failure", error.text());

        Assert.assertTrue(RespClientCodec.readReply(in("_\r\n"), 1024).isNull());
        Assert.assertTrue(RespClientCodec.readReply(in("*-1\r\n"), 1024).isNull());

        RespClientCodec.RespReply nested = RespClientCodec.readReply(
                in("*2\r\n*1\r\n+OK\r\n$0\r\n\r\n"),
                1024
        );
        Assert.assertEquals(2, nested.values().size());
        RespClientCodec.RespReply simple = nested.values().get(0).values().get(0);
        Assert.assertEquals(RespClientCodec.RespReply.Kind.SIMPLE_STRING, simple.kind());
        Assert.assertEquals("OK", simple.text());
        Assert.assertEquals(0, nested.values().get(1).bytes().length);
        Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, nested.kind());
        Assert.assertNull(error.bytes());
    }

    @Test
    public void readsResp3MapsAndSetsWithoutLosingAggregateKind() throws Exception {
        RespClientCodec.RespReply map = RespClientCodec.readReply(
                in("%1\r\n+key\r\n~2\r\n$1\r\na\r\n$1\r\nb\r\n"), 1024);

        Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, map.kind());
        Assert.assertEquals(2, map.values().size());
        RespClientCodec.RespReply set = map.values().get(1);
        Assert.assertEquals(RespClientCodec.RespReply.Kind.SET, set.kind());
        Assert.assertEquals(2, set.values().size());
        Assert.assertArrayEquals(bytes("a"), set.values().get(0).bytes());
    }

    @Test
    public void rejectsInvalidLimitsTypesAndTruncatedReplies() {
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> RespClientCodec.readReply(in("+OK\r\n"), -1)
        );
        assertReadFails("", 1024, "unexpected EOF before RESP reply");
        assertReadFails("?unknown\r\n", 1024, "unexpected RESP reply type");
        assertReadFails("+unterminated", 1024, "unexpected EOF before RESP line terminator");
        assertReadFails("$3\r\nab", 1024, "unexpected EOF in RESP bulk string");
        assertReadFails("$3\r\nabcxx", 1024, "expected RESP CRLF");
        assertReadFails("$4\r\nabcd\r\n", 3, "exceeds limit");
        assertReadFails("_xx", 1024, "expected RESP CRLF");
    }

    @Test
    public void rejectsMalformedAndOverflowingNumbers() {
        assertReadFails(":x\r\n", 1024, "invalid RESP integer");
        assertReadFails(":-\r\n", 1024, "invalid RESP integer");
        assertReadFails(":1x\r\n", 1024, "invalid RESP integer");
        assertReadFails(":1\rx", 1024, "expected RESP CRLF");
        assertReadFails(":999999999999999999999999\r\n", 1024, "invalid RESP integer");
        assertReadFails("$2147483648\r\n", 1024, "invalid RESP bulk string length");
        assertReadFails("*2147483648\r\n", 1024, "invalid RESP array length");
    }

    @Test
    public void replyBytesAndValuesAreDefensiveCopies() throws Exception {
        RespClientCodec.RespReply bulk = RespClientCodec.readReply(in("$3\r\nabc\r\n"), 1024);
        byte[] first = bulk.bytes();
        first[0] = 'x';
        Assert.assertArrayEquals(bytes("abc"), bulk.bytes());

        byte[] source = bytes("value");
        RespClientCodec.RespReply constructed = new RespClientCodec.RespReply(
                RespClientCodec.RespReply.Kind.BULK_STRING,
                null,
                source,
                null,
                null
        );
        source[0] = 'x';
        Assert.assertArrayEquals(bytes("value"), constructed.bytes());

        RespClientCodec.RespReply array = new RespClientCodec.RespReply(
                RespClientCodec.RespReply.Kind.ARRAY,
                null,
                null,
                null,
                new java.util.ArrayList<>(List.of(constructed))
        );
        Assert.assertThrows(UnsupportedOperationException.class, () -> array.values().clear());
    }

    private static void assertReadFails(String payload, int maxBulkBytes, String expectedMessage) {
        IOException failure = Assert.assertThrows(
                IOException.class,
                () -> RespClientCodec.readReply(in(payload), maxBulkBytes)
        );
        Assert.assertTrue(
                "expected <" + failure.getMessage() + "> to contain <" + expectedMessage + ">",
                failure.getMessage().contains(expectedMessage)
        );
    }

    private static ByteArrayInputStream in(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static String ascii(byte[] bytes) {
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
