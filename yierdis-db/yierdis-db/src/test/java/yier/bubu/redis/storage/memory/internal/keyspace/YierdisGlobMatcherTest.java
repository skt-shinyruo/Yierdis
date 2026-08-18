package yier.bubu.redis.storage.memory.internal.keyspace;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;

import java.nio.charset.StandardCharsets;

public class YierdisGlobMatcherTest {
    @Test
    public void matchesLiteralStarAndQuestionAcrossInputBackings() {
        assertMatchesBoth(true, "user:123", "user:123");
        assertMatchesBoth(false, "user:123", "user:124");
        assertMatchesBoth(true, "user:*", "user:123");
        assertMatchesBoth(true, "a*b*c", "axbyc");
        assertMatchesBoth(false, "a*b*c", "axbyd");
        assertMatchesBoth(true, "user:??", "user:ab");
        assertMatchesBoth(false, "user:??", "user:a");
    }

    @Test
    public void matchesCharacterClassesNegationAndRangesAcrossInputBackings() {
        assertMatchesBoth(true, "key[0-9]", "key7");
        assertMatchesBoth(false, "key[0-9]", "keyx");
        assertMatchesBoth(true, "key[^0-9]", "keyx");
        assertMatchesBoth(false, "key[^0-9]", "key7");
        assertMatchesBoth(true, "key[!a-c]", "keyz");
        assertMatchesBoth(true, "key[z-a]", "keym");
    }

    @Test
    public void matchesEscapesAndMalformedClassesAcrossInputBackings() {
        assertMatchesBoth(true, "a\\*b", "a*b");
        assertMatchesBoth(false, "a\\*b", "axxb");
        assertMatchesBoth(true, "a\\", "a\\");
        assertMatchesBoth(true, "a[", "a[");
        assertMatchesBoth(true, "a[]]", "a]");
    }

    @Test
    public void matchesEmptyTextAndTrailingStarsAcrossInputBackings() {
        assertMatchesBoth(true, "", "");
        assertMatchesBoth(true, "*", "");
        assertMatchesBoth(true, "***", "");
        assertMatchesBoth(true, "a**", "a");
        assertMatchesBoth(false, "?", "");
        assertMatchesBoth(false, "*a", "");
    }

    @Test
    public void rejectsNullInputsAndNegativeLengthViews() {
        Assert.assertFalse(YierdisGlobMatcher.matches(null, b("x")));
        Assert.assertFalse(YierdisGlobMatcher.matches(null, view(b("x"))));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("*"), (byte[]) null));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("*"), (BytesView) null));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("*"), new BytesView() {
            @Override
            public int length() {
                return -1;
            }

            @Override
            public byte getByte(int index) {
                throw new AssertionError("negative length view must not be read");
            }
        }));
    }

    private static void assertMatchesBoth(boolean expected, String pattern, String text) {
        byte[] patternBytes = b(pattern);
        byte[] textBytes = b(text);
        Assert.assertEquals("byte[] path", expected, YierdisGlobMatcher.matches(patternBytes, textBytes));
        Assert.assertEquals("BytesView path", expected, YierdisGlobMatcher.matches(patternBytes, view(textBytes)));
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesView view(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }
}
