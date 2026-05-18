package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class InlineCommandParserTest {
    @Test
    public void decodesQuotedEscapesAndHexBytes() {
        InlineCommandParser.Decoded decoded = parse("SET \"a\\x20b\" \"\\x41\\n\"");

        Assert.assertEquals(3, decoded.argc());
        Assert.assertArrayEquals(bytes("SET"), decoded.copyArg(0));
        Assert.assertArrayEquals(bytes("a b"), decoded.copyArg(1));
        Assert.assertArrayEquals(new byte[]{'A', '\n'}, decoded.copyArg(2));
        Assert.assertEquals(8, decoded.retainedBytes());
    }

    @Test
    public void parseUnlimitedAllowsCallersToApplyTheirOwnLimit() {
        InlineCommandParser.Decoded decoded = InlineCommandParser.parseUnlimited(bytes("A B"), 0, 3);

        Assert.assertEquals(2, decoded.argc());
        Assert.assertArrayEquals(bytes("A"), decoded.copyArg(0));
        Assert.assertArrayEquals(bytes("B"), decoded.copyArg(1));
    }

    @Test
    public void splitsUtf8IntoCopiedArgs() {
        List<byte[]> args = InlineCommandParser.splitUtf8("ECHO \"hi\"", 4);

        Assert.assertEquals(2, args.size());
        Assert.assertArrayEquals(bytes("ECHO"), args.get(0));
        Assert.assertArrayEquals(bytes("hi"), args.get(1));
    }

    @Test
    public void rejectsTooManyArgsWhenLimitIsPositive() {
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> InlineCommandParser.parse(bytes("A B"), 0, 3, 1)
        );
    }

    private static InlineCommandParser.Decoded parse(String value) {
        byte[] bytes = bytes(value);
        return InlineCommandParser.parse(bytes, 0, bytes.length, 16);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
