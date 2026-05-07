package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;

import java.nio.charset.StandardCharsets;

public class YierdisGlobMatcherTest {
    @Test
    public void matchesLiteralStarQuestionAndBytesView() {
        Assert.assertTrue(YierdisGlobMatcher.matches(b("user:*"), b("user:123")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("user:??"), b("user:ab")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("user:??"), b("user:a")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a*c"), view(b("abbbc"))));
    }

    @Test
    public void matchesCharacterClassesNegationAndRanges() {
        Assert.assertTrue(YierdisGlobMatcher.matches(b("key[0-9]"), b("key7")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("key[0-9]"), b("keyx")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("key[^0-9]"), b("keyx")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("key[^0-9]"), b("key7")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("key[!a-c]"), b("keyz")));
    }

    @Test
    public void matchesEscapesAndMalformedClassesLikeCurrentDbMatcher() {
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a\\*b"), b("a*b")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("a\\*b"), b("axxb")));
        Assert.assertTrue(YierdisGlobMatcher.matches(new byte[]{'a', '\\'}, new byte[]{'a', '\\'}));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a["), b("a[")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a[]]"), b("a]")));
    }

    @Test
    public void rejectsNullInputsAndNegativeLengthViews() {
        Assert.assertFalse(YierdisGlobMatcher.matches(null, b("x")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("*"), (byte[]) null));
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
