package yier.bubu.redis.protocol.custom.v1.wire;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CustomProtocolV1ReplyInspectorTest {
    @Test
    public void matchesOkAsciiStringResultAcceptsCanonicalReplies() {
        Assert.assertTrue(
                CustomProtocolV1ReplyInspector.matchesOkAsciiStringResult(
                        utf8("{\"ok\":true,\"result\":\"PONG\"}\n"),
                        0,
                        utf8("{\"ok\":true,\"result\":\"PONG\"}\n").length,
                        ascii("PONG")
                )
        );
        Assert.assertTrue(
                CustomProtocolV1ReplyInspector.matchesOkAsciiStringResult(
                        utf8("{\"ok\":true,\"result\":\"OK\"}"),
                        0,
                        utf8("{\"ok\":true,\"result\":\"OK\"}").length,
                        ascii("OK")
                )
        );
    }

    @Test
    public void decodedOkResultByteLengthAcceptsEscapedUtf8Strings() {
        byte[] line = utf8("{\"ok\":true,\"result\":\"line1\\\\line2\\n中文\"}\n");
        int expectedLen = utf8("line1\\line2\n中文").length;

        Assert.assertEquals(
                expectedLen,
                CustomProtocolV1ReplyInspector.decodedOkResultByteLength(line, 0, line.length)
        );
    }

    @Test
    public void decodedOkResultByteLengthAcceptsTaggedB64Bytes() {
        byte[] line = utf8("{\"ok\":true,\"result\":{\"$b64\":\"wyg=\"}}");

        Assert.assertEquals(
                2,
                CustomProtocolV1ReplyInspector.decodedOkResultByteLength(line, 0, line.length)
        );
    }

    @Test
    public void decodedOkResultByteLengthReturnsNullSentinelForNull() {
        byte[] line = utf8("{\"ok\":true,\"result\":null}\n");

        Assert.assertEquals(
                CustomProtocolV1ReplyInspector.NULL_RESULT,
                CustomProtocolV1ReplyInspector.decodedOkResultByteLength(line, 0, line.length)
        );
    }

    @Test
    public void decodedOkResultByteLengthRejectsMalformedTaggedB64() {
        byte[] line = utf8("{\"ok\":true,\"result\":{\"$b64\":\"wy=g\"}}");

        Assert.assertEquals(
                CustomProtocolV1ReplyInspector.INVALID_RESULT,
                CustomProtocolV1ReplyInspector.decodedOkResultByteLength(line, 0, line.length)
        );
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
