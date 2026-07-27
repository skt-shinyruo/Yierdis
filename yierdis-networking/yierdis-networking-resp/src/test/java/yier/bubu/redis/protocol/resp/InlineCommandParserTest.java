package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        InlineCommandParser.Decoded singleQuoted = parse("ECHO 'it\\'s' ''");
        Assert.assertArrayEquals(bytes("it's"), singleQuoted.copyArg(1));
        Assert.assertArrayEquals(new byte[0], singleQuoted.copyArg(2));

        InlineCommandParser.Decoded escaped = parse("ECHO \"\\n\\r\\t\\b\\a\\z\\x41\\x4a\\x4A\"");
        Assert.assertArrayEquals(
                new byte[]{'\n', '\r', '\t', '\b', 7, 'z', 'A', 'J', 'J'},
                escaped.copyArg(1)
        );
    }

    @Test
    public void growsMetadataPastSixteenArgumentsAndChecksEveryAccessorBoundary() {
        List<String> expected = new ArrayList<>();
        StringBuilder line = new StringBuilder();
        for (int index = 0; index < 21; index++) {
            String argument = "a" + index;
            expected.add(argument);
            if (index > 0) {
                line.append(' ');
            }
            line.append(argument);
        }

        byte[] input = bytes(line.toString());
        InlineCommandParser.Decoded decoded = InlineCommandParser.parseUnlimited(input, 0, input.length);

        Assert.assertEquals(expected.size(), decoded.argc());
        int retainedBytes = expected.stream().mapToInt(String::length).sum();
        Assert.assertEquals(retainedBytes, decoded.decodedLen());
        Assert.assertEquals(retainedBytes, decoded.retainedBytes());
        Assert.assertSame(decoded.decoded(), decoded.decoded());
        Assert.assertEquals(0, decoded.offset(0));
        Assert.assertEquals(2, decoded.length(0));
        Assert.assertArrayEquals(bytes("a20"), decoded.copyArgs()[20]);
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> decoded.offset(-1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> decoded.length(decoded.argc()));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> decoded.copyArg(decoded.argc()));
    }

    private static InlineCommandParser.Decoded parse(String value) {
        byte[] bytes = bytes(value);
        return InlineCommandParser.parse(bytes, 0, bytes.length, 16);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
