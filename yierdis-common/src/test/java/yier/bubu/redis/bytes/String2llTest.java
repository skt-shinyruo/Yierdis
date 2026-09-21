package yier.bubu.redis.bytes;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class String2llTest {
    @Test
    public void acceptsCanonicalDecimalIntegers() {
        Assert.assertEquals(0L, String2ll.parse(bytes("0")));
        Assert.assertEquals(1L, String2ll.parse(bytes("1")));
        Assert.assertEquals(-1L, String2ll.parse(bytes("-1")));
        Assert.assertEquals(123456789L, String2ll.parse(bytes("123456789")));
        Assert.assertEquals(-41L, String2ll.parse(bytes("-41")));
        Assert.assertEquals(Long.MAX_VALUE, String2ll.parse(bytes("9223372036854775807")));
        Assert.assertEquals(Long.MIN_VALUE, String2ll.parse(bytes("-9223372036854775808")));

        // tryParse 与 parse 接受集一致，只是以 OptionalLong 返回。
        Assert.assertEquals(0L, String2ll.tryParse(bytes("0")).orElseThrow());
        Assert.assertEquals(-1L, String2ll.tryParse(bytes("-1")).orElseThrow());
        Assert.assertEquals(Long.MAX_VALUE, String2ll.tryParse(bytes("9223372036854775807")).orElseThrow());
        Assert.assertEquals(Long.MIN_VALUE, String2ll.tryParse(bytes("-9223372036854775808")).orElseThrow());
    }

    @Test
    public void rejectsWhatRedisString2llRejects() {
        // 与 Redis string2ll 对齐的拒绝集合：空、仅符号、'+' 前缀、前导零、-0、空白、
        // 非数字字符（含高位字节）以及双向溢出。
        List<byte[]> rejected = List.of(
                bytes(""),
                bytes("+"),
                bytes("-"),
                bytes("+1"),
                bytes("+0"),
                bytes("00"),
                bytes("01"),
                bytes("-0"),
                bytes("-01"),
                bytes(" 1"),
                bytes("1 "),
                bytes("1x"),
                bytes("x1"),
                bytes("9_000"),
                bytes("9223372036854775808"),
                bytes("-9223372036854775809"),
                bytes("99999999999999999999999"),
                new byte[]{(byte) 0xC2, (byte) 0xB1},
                new byte[]{'1', (byte) 0x80}
        );
        for (byte[] value : rejected) {
            assertRejected(value);
        }
        assertRejected(null);
    }

    private static void assertRejected(byte[] value) {
        String description = value == null ? "null" : new String(value, StandardCharsets.US_ASCII);
        Assert.assertThrows(
                "expected rejection of " + description,
                NumberFormatException.class,
                () -> String2ll.parse(value)
        );
        Assert.assertTrue("expected empty OptionalLong for " + description, String2ll.tryParse(value).isEmpty());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
