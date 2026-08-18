package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class InlineCommandParserTest {
    @Test
    public void decodesQuotedEscapesAndHexBytes() {
        byte[][] args = parse("SET \"a\\x20b\" \"\\x41\\n\"");

        Assert.assertEquals(3, args.length);
        Assert.assertArrayEquals(bytes("SET"), args[0]);
        Assert.assertArrayEquals(bytes("a b"), args[1]);
        Assert.assertArrayEquals(new byte[]{'A', '\n'}, args[2]);
    }

    @Test
    public void parseUnlimitedAllowsCallersToApplyTheirOwnLimit() {
        byte[][] args = InlineCommandParser.parseUnlimited(bytes("A B"), 0, 3);

        Assert.assertEquals(2, args.length);
        Assert.assertArrayEquals(bytes("A"), args[0]);
        Assert.assertArrayEquals(bytes("B"), args[1]);
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

    @Test
    public void rejectsInvalidLimitsInputsAndOverflowingRanges() {
        byte[] input = bytes("PING");

        Assert.assertThrows(IllegalArgumentException.class,
                () -> InlineCommandParser.parse(input, 0, input.length, 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> InlineCommandParser.parse(input, 0, input.length, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> InlineCommandParser.parseUnlimited(null, 0, 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> InlineCommandParser.splitUtf8(null, 1));
        Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> InlineCommandParser.parseUnlimited(input, -1, 1));
        Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> InlineCommandParser.parseUnlimited(input, 0, -1));
        Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> InlineCommandParser.parseUnlimited(input, input.length, 1));
        Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> InlineCommandParser.parseUnlimited(input, Integer.MAX_VALUE, 2));
    }

    @Test
    public void rejectsEmptyInputUnbalancedQuotesAndAdjacentQuotedTokens() {
        Assert.assertThrows(IllegalArgumentException.class, () -> parse(" \t "));
        Assert.assertThrows(IllegalArgumentException.class, () -> parse("ECHO \"unterminated"));
        Assert.assertThrows(IllegalArgumentException.class, () -> parse("ECHO 'unterminated"));
        Assert.assertThrows(IllegalArgumentException.class, () -> parse("ECHO \"a\"b"));
        Assert.assertThrows(IllegalArgumentException.class, () -> parse("ECHO 'a'b"));
    }

    @Test
    public void decodesSingleQuotesAndEverySupportedDoubleQuoteEscape() {
        byte[][] singleQuoted = parse("ECHO 'it\\'s' ''");
        Assert.assertArrayEquals(bytes("it's"), singleQuoted[1]);
        Assert.assertArrayEquals(new byte[0], singleQuoted[2]);

        byte[][] escaped = parse("ECHO \"\\n\\r\\t\\b\\a\\z\\x41\\x4a\\x4A\"");
        Assert.assertArrayEquals(
                new byte[]{'\n', '\r', '\t', '\b', 7, 'z', 'A', 'J', 'J'},
                escaped[1]
        );
    }

    @Test
    public void growsMetadataPastSixteenArguments() {
        StringBuilder line = new StringBuilder();
        for (int index = 0; index < 21; index++) {
            if (index > 0) {
                line.append(' ');
            }
            line.append('a').append(index);
        }

        byte[] input = bytes(line.toString());
        byte[][] args = InlineCommandParser.parseUnlimited(input, 0, input.length);

        Assert.assertEquals(21, args.length);
        Assert.assertArrayEquals(bytes("a20"), args[20]);
    }

    private static byte[][] parse(String value) {
        byte[] bytes = bytes(value);
        return InlineCommandParser.parse(bytes, 0, bytes.length, 16);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
