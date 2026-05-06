package yier.bubu.redis.protocol.v1;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CustomProtocolV1RequestPayloadParserTest {
    @Test
    public void parseDecodesUtf8ArgsAndNullsIntoArgvBytes() {
        byte[] payload = utf8("{\"cmd\":\" \\tPING\\r\\n \",\"args\":[\"alpha\",null,\"你好\",\"\"]}");

        CustomProtocolV1ArgvRequest request =
                CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 16);

        Assert.assertEquals(5, request.argc());
        Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));
        Assert.assertArrayEquals(utf8("alpha"), request.readOnlyArg(1));
        Assert.assertTrue(request.isNull(2));
        Assert.assertArrayEquals(utf8("你好"), request.readOnlyArg(3));
        Assert.assertArrayEquals(utf8(""), request.readOnlyArg(4));
        Assert.assertEquals(15, request.retainedBytes());
    }

    @Test
    public void parseDecodesEscapesAndUnicodeEscapesIntoUtf8Bytes() {
        byte[] payload = utf8("{\"cmd\":\"ECHO\",\"args\":[\"line1\\\\line2\\n中文\",\"\\u4F60\\u597D\",\"\"]}");

        CustomProtocolV1ArgvRequest request =
                CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 16);

        Assert.assertEquals(4, request.argc());
        Assert.assertArrayEquals(utf8("ECHO"), request.readOnlyArg(0));
        Assert.assertArrayEquals(utf8("line1\\line2\n中文"), request.readOnlyArg(1));
        Assert.assertArrayEquals(utf8("你好"), request.readOnlyArg(2));
        Assert.assertArrayEquals(utf8(""), request.readOnlyArg(3));
        Assert.assertEquals(28, request.retainedBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseRejectsNonStringNonNullArgs() {
        byte[] payload = utf8("{\"cmd\":\"PING\",\"args\":[1]}");

        CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 16);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseRejectsTooManyArgs() {
        byte[] payload = utf8("{\"cmd\":\"PING\",\"args\":[\"a\",\"b\"]}");

        CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseRejectsUnknownFields() {
        byte[] payload = utf8("{\"cmd\":\"PING\",\"args\":[],\"extra\":true}");

        CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 16);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
