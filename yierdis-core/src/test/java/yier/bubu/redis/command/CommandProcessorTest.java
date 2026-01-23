package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandProcessorTest {
    @Test
    public void stringIsBinarySafe() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("k");
            byte[] value = new byte[]{0, (byte) 0xFF, 'a', '\n'};

            Assert.assertTrue(client.execute(Arrays.asList(
                    b("SET"),
                    key,
                    value
            )) instanceof RespSimpleString);

            RespObject get = client.execute(Arrays.asList(
                    b("GET"),
                    key
            ));
            Assert.assertTrue(get instanceof RespBulkString);
            Assert.assertArrayEquals(value, ((RespBulkString) get).data());

            RespInteger len = (RespInteger) client.execute(Arrays.asList(
                    b("STRLEN"),
                    key
            ));
            Assert.assertEquals(value.length, len.value());

            byte[] extra = new byte[]{1, 2, 3};
            RespInteger newLen = (RespInteger) client.execute(Arrays.asList(
                    b("APPEND"),
                    key,
                    extra
            ));
            Assert.assertEquals(value.length + extra.length, newLen.value());

            RespObject get2 = client.execute(Arrays.asList(
                    b("GET"),
                    key
            ));
            Assert.assertTrue(get2 instanceof RespBulkString);
            Assert.assertArrayEquals(new byte[]{0, (byte) 0xFF, 'a', '\n', 1, 2, 3}, ((RespBulkString) get2).data());
            }
        });
    }

    @Test
    public void incrWorksAfterAppendWhenRawStringHasSpareCapacity() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            Assert.assertTrue(client.execute(cmd("SET", "k", "1")) instanceof RespSimpleString);

            RespInteger len = (RespInteger) client.execute(Arrays.asList(b("APPEND"), b("k"), b("0")));
            Assert.assertEquals(2, len.value());

            RespInteger incr = (RespInteger) client.execute(Arrays.asList(b("INCR"), b("k")));
            Assert.assertEquals(11, incr.value());

            RespBulkString get = (RespBulkString) client.execute(Arrays.asList(b("GET"), b("k")));
            Assert.assertEquals("11", get.asString());
            }
        });
    }

    @Test
    public void integerLikeStringsAreBinarySafe() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            assertBinarySafeRoundTrip(client, b("k:01"), b("01"));
            assertBinarySafeRoundTrip(client, b("k:+1"), b("+1"));
            assertBinarySafeRoundTrip(client, b("k:-0"), b("-0"));
            }
        });
    }

    @Test
    public void binaryKeyIsSupportedEndToEnd() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
            byte[] value = new byte[]{1, 2, 3};

            Assert.assertTrue(client.execute(Arrays.asList(b("SET"), key, value)) instanceof RespSimpleString);

            RespBulkString get = (RespBulkString) client.execute(Arrays.asList(b("GET"), key));
            Assert.assertArrayEquals(value, get.data());

            RespInteger exists1 = (RespInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(1, exists1.value());

            RespArray keys = (RespArray) client.execute(Arrays.asList(b("KEYS"), b("*")));
            Assert.assertTrue(containsBytes(keys, key));

            RespInteger del = (RespInteger) client.execute(Arrays.asList(b("DEL"), key));
            Assert.assertEquals(1, del.value());

            RespInteger exists0 = (RespInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists0.value());
            }
        });
    }

    @Test
    public void keysGlobMatchesOnRawBytes() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] v = new byte[]{1};
            byte[] k1 = new byte[]{0, 'a'};
            byte[] k2 = new byte[]{0, 'b'};
            byte[] k3 = new byte[]{1, 'a'};

            client.execute(Arrays.asList(b("SET"), k1, v));
            client.execute(Arrays.asList(b("SET"), k2, v));
            client.execute(Arrays.asList(b("SET"), k3, v));

            // Prefix match: 0x00 + '*'
            RespArray prefix = (RespArray) client.execute(Arrays.asList(
                    b("KEYS"),
                    new byte[]{0, '*'}
            ));
            Assert.assertEquals(2, prefix.values().size());
            Assert.assertTrue(containsBytes(prefix, k1));
            Assert.assertTrue(containsBytes(prefix, k2));

            // Exactly 2 bytes: 0x00 + '?'
            RespArray oneByte = (RespArray) client.execute(Arrays.asList(
                    b("KEYS"),
                    new byte[]{0, '?'}
            ));
            Assert.assertEquals(2, oneByte.values().size());
            Assert.assertTrue(containsBytes(oneByte, k1));
            Assert.assertTrue(containsBytes(oneByte, k2));
            }
        });
    }

    @Test
    public void keysGlobSupportsBracketsNegationRangesAndEscapes() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] v = new byte[]{1};
            byte[] ka1 = b("a1");
            byte[] kb1 = b("b1");
            byte[] kc1 = b("c1");
            byte[] kStar = b("*");
            byte[] kQ = b("?");
            byte[] kLbracket = b("[");

            client.execute(Arrays.asList(b("SET"), ka1, v));
            client.execute(Arrays.asList(b("SET"), kb1, v));
            client.execute(Arrays.asList(b("SET"), kc1, v));
            client.execute(Arrays.asList(b("SET"), kStar, v));
            client.execute(Arrays.asList(b("SET"), kQ, v));
            client.execute(Arrays.asList(b("SET"), kLbracket, v));

            RespArray ab = (RespArray) client.execute(cmd("KEYS", "[ab]*"));
            Assert.assertEquals(2, ab.values().size());
            Assert.assertTrue(containsBytes(ab, ka1));
            Assert.assertTrue(containsBytes(ab, kb1));

            RespArray aToC = (RespArray) client.execute(cmd("KEYS", "[a-c]*"));
            Assert.assertEquals(3, aToC.values().size());
            Assert.assertTrue(containsBytes(aToC, ka1));
            Assert.assertTrue(containsBytes(aToC, kb1));
            Assert.assertTrue(containsBytes(aToC, kc1));

            RespArray notA = (RespArray) client.execute(cmd("KEYS", "[^a]*"));
            Assert.assertTrue(containsBytes(notA, kb1));
            Assert.assertTrue(containsBytes(notA, kc1));
            Assert.assertFalse(containsBytes(notA, ka1));

            // Escapes: "\*" "\?" "\[" should match literal '*', '?', '['.
            RespArray stars = (RespArray) client.execute(cmd("KEYS", "\\*"));
            Assert.assertEquals(1, stars.values().size());
            Assert.assertTrue(containsBytes(stars, kStar));

            RespArray qs = (RespArray) client.execute(cmd("KEYS", "\\?"));
            Assert.assertEquals(1, qs.values().size());
            Assert.assertTrue(containsBytes(qs, kQ));

            RespArray lbrackets = (RespArray) client.execute(cmd("KEYS", "\\["));
            Assert.assertEquals(1, lbrackets.values().size());
            Assert.assertTrue(containsBytes(lbrackets, kLbracket));

            // Unclosed character classes are treated as literal '['.
            RespArray literalLbracket = (RespArray) client.execute(cmd("KEYS", "["));
            Assert.assertEquals(1, literalLbracket.values().size());
            Assert.assertTrue(containsBytes(literalLbracket, kLbracket));
            }
        });
    }

    @Test
    public void incrErrorsOnNonIntegerOrOverflow() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("k");

            // Non-integer
            client.execute(Arrays.asList(b("SET"), key, new byte[]{'x'}));
            RespObject err1 = client.execute(Arrays.asList(b("INCR"), key));
            Assert.assertTrue(err1 instanceof RespError);
            Assert.assertEquals("ERR value is not an integer or out of range", ((RespError) err1).message());

            // Overflow
            client.execute(Arrays.asList(b("SET"), key, b(Long.toString(Long.MAX_VALUE))));
            RespObject err2 = client.execute(Arrays.asList(b("INCR"), key));
            Assert.assertTrue(err2 instanceof RespError);
            Assert.assertEquals("ERR value is not an integer or out of range", ((RespError) err2).message());

            }
        });
    }

    @Test
    public void expireZeroDeletesKeyImmediately() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{0, (byte) 0xFF};
            byte[] value = new byte[]{1, 2, 3};

            client.execute(Arrays.asList(b("SET"), key, value));

            RespInteger expire = (RespInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")));
            Assert.assertEquals(1, expire.value());

            RespInteger ttl = (RespInteger) client.execute(Arrays.asList(b("TTL"), key));
            Assert.assertEquals(-2, ttl.value());

            RespBulkString get = (RespBulkString) client.execute(Arrays.asList(b("GET"), key));
            Assert.assertTrue(get.isNull());

            RespInteger exists = (RespInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            }
        });
    }

    @Test
    public void setGetIncrExpireTtl() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            Assert.assertTrue(client.execute(cmd("SET", "a", "1")) instanceof RespSimpleString);
            RespObject get = client.execute(cmd("GET", "a"));
            Assert.assertEquals("1", ((RespBulkString) get).asString());

            RespObject incr = client.execute(cmd("INCR", "a"));
            Assert.assertEquals(2, ((RespInteger) incr).value());

            RespObject expire = client.execute(cmd("EXPIRE", "a", "10"));
            Assert.assertEquals(1, ((RespInteger) expire).value());

            RespObject ttl = client.execute(cmd("TTL", "a"));
            Assert.assertTrue(((RespInteger) ttl).value() >= 0);

            }
        });
    }

    @Test
    public void commandParsingIsLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            forEachDb(db -> {
                YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
                try (FastTestClient client = new FastTestClient(processor)) {

                RespObject pong = client.execute(cmd("ping"));
                Assert.assertTrue(pong instanceof RespSimpleString);
                Assert.assertEquals("PONG", ((RespSimpleString) pong).value());

                client.execute(cmd("set", "a", "1"));
                RespObject type = client.execute(cmd("type", "a"));
                Assert.assertTrue(type instanceof RespSimpleString);
                Assert.assertEquals("string", ((RespSimpleString) type).value());
                }
            });
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void setNxReturnsNilWhenKeyExists() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof RespSimpleString);
            RespObject res = client.execute(cmd("SET", "k", "v2", "NX"));
            Assert.assertTrue(res instanceof RespBulkString);
            Assert.assertTrue(((RespBulkString) res).isNull());

            }
        });
    }

    @Test
    public void wrongTypeReturnsError() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            client.execute(cmd("SET", "a", "1"));
            RespObject err = client.execute(cmd("LPUSH", "a", "x"));
            Assert.assertTrue(err instanceof RespError);
            Assert.assertTrue(((RespError) err).message().startsWith("WRONGTYPE"));

            }
        });
    }

    private static void assertBinarySafeRoundTrip(FastTestClient client, byte[] key, byte[] value) {
        Assert.assertTrue(client.execute(Arrays.asList(b("SET"), key, value)) instanceof RespSimpleString);

        RespBulkString get = (RespBulkString) client.execute(Arrays.asList(b("GET"), key));
        Assert.assertArrayEquals(value, get.data());

        RespInteger len = (RespInteger) client.execute(Arrays.asList(b("STRLEN"), key));
        Assert.assertEquals(value.length, len.value());
    }

    private static boolean containsBytes(RespArray array, byte[] expected) {
        for (RespObject o : array.values()) {
            if (o instanceof RespBulkString && Arrays.equals(expected, ((RespBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }
}
