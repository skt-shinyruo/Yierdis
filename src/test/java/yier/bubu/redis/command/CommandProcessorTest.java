package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
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

public class CommandProcessorTest {
    @Test
    public void stringIsBinarySafe() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("k");
        byte[] value = new byte[]{0, (byte) 0xFF, 'a', '\n'};

        Assert.assertTrue(cp.execute(Arrays.asList(
                b("SET"),
                key,
                value
        )) instanceof RespSimpleString);

        RespObject get = cp.execute(Arrays.asList(
                b("GET"),
                key
        ));
        Assert.assertTrue(get instanceof RespBulkString);
        Assert.assertArrayEquals(value, ((RespBulkString) get).data());

        RespInteger len = (RespInteger) cp.execute(Arrays.asList(
                b("STRLEN"),
                key
        ));
        Assert.assertEquals(value.length, len.value());

        byte[] extra = new byte[]{1, 2, 3};
        RespInteger newLen = (RespInteger) cp.execute(Arrays.asList(
                b("APPEND"),
                key,
                extra
        ));
        Assert.assertEquals(value.length + extra.length, newLen.value());

        RespObject get2 = cp.execute(Arrays.asList(
                b("GET"),
                key
        ));
        Assert.assertTrue(get2 instanceof RespBulkString);
        Assert.assertArrayEquals(new byte[]{0, (byte) 0xFF, 'a', '\n', 1, 2, 3}, ((RespBulkString) get2).data());

        db.shutdown();
    }

    @Test
    public void incrWorksAfterAppendWhenRawStringHasSpareCapacity() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        Assert.assertTrue(cp.execute(cmd("SET", "k", "1")) instanceof RespSimpleString);

        RespInteger len = (RespInteger) cp.execute(Arrays.asList(b("APPEND"), b("k"), b("0")));
        Assert.assertEquals(2, len.value());

        RespInteger incr = (RespInteger) cp.execute(Arrays.asList(b("INCR"), b("k")));
        Assert.assertEquals(11, incr.value());

        RespBulkString get = (RespBulkString) cp.execute(Arrays.asList(b("GET"), b("k")));
        Assert.assertEquals("11", get.asString());

        db.shutdown();
    }

    @Test
    public void integerLikeStringsAreBinarySafe() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        assertBinarySafeRoundTrip(cp, b("k:01"), b("01"));
        assertBinarySafeRoundTrip(cp, b("k:+1"), b("+1"));
        assertBinarySafeRoundTrip(cp, b("k:-0"), b("-0"));

        db.shutdown();
    }

    @Test
    public void binaryKeyIsSupportedEndToEnd() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
        byte[] value = new byte[]{1, 2, 3};

        Assert.assertTrue(cp.execute(Arrays.asList(b("SET"), key, value)) instanceof RespSimpleString);

        RespBulkString get = (RespBulkString) cp.execute(Arrays.asList(b("GET"), key));
        Assert.assertArrayEquals(value, get.data());

        RespInteger exists1 = (RespInteger) cp.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(1, exists1.value());

        RespArray keys = (RespArray) cp.execute(Arrays.asList(b("KEYS"), b("*")));
        Assert.assertTrue(containsBytes(keys, key));

        RespInteger del = (RespInteger) cp.execute(Arrays.asList(b("DEL"), key));
        Assert.assertEquals(1, del.value());

        RespInteger exists0 = (RespInteger) cp.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists0.value());

        db.shutdown();
    }

    @Test
    public void keysGlobMatchesOnRawBytes() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] v = new byte[]{1};
        byte[] k1 = new byte[]{0, 'a'};
        byte[] k2 = new byte[]{0, 'b'};
        byte[] k3 = new byte[]{1, 'a'};

        cp.execute(Arrays.asList(b("SET"), k1, v));
        cp.execute(Arrays.asList(b("SET"), k2, v));
        cp.execute(Arrays.asList(b("SET"), k3, v));

        // Prefix match: 0x00 + '*'
        RespArray prefix = (RespArray) cp.execute(Arrays.asList(
                b("KEYS"),
                new byte[]{0, '*'}
        ));
        Assert.assertEquals(2, prefix.values().size());
        Assert.assertTrue(containsBytes(prefix, k1));
        Assert.assertTrue(containsBytes(prefix, k2));

        // Exactly 2 bytes: 0x00 + '?'
        RespArray oneByte = (RespArray) cp.execute(Arrays.asList(
                b("KEYS"),
                new byte[]{0, '?'}
        ));
        Assert.assertEquals(2, oneByte.values().size());
        Assert.assertTrue(containsBytes(oneByte, k1));
        Assert.assertTrue(containsBytes(oneByte, k2));

        db.shutdown();
    }

    @Test
    public void incrErrorsOnNonIntegerOrOverflow() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("k");

        // Non-integer
        cp.execute(Arrays.asList(b("SET"), key, new byte[]{'x'}));
        RespObject err1 = cp.execute(Arrays.asList(b("INCR"), key));
        Assert.assertTrue(err1 instanceof RespError);
        Assert.assertEquals("ERR value is not an integer or out of range", ((RespError) err1).message());

        // Overflow
        cp.execute(Arrays.asList(b("SET"), key, b(Long.toString(Long.MAX_VALUE))));
        RespObject err2 = cp.execute(Arrays.asList(b("INCR"), key));
        Assert.assertTrue(err2 instanceof RespError);
        Assert.assertEquals("ERR value is not an integer or out of range", ((RespError) err2).message());

        db.shutdown();
    }

    @Test
    public void expireZeroDeletesKeyImmediately() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = new byte[]{0, (byte) 0xFF};
        byte[] value = new byte[]{1, 2, 3};

        cp.execute(Arrays.asList(b("SET"), key, value));

        RespInteger expire = (RespInteger) cp.execute(Arrays.asList(b("EXPIRE"), key, b("0")));
        Assert.assertEquals(1, expire.value());

        RespInteger ttl = (RespInteger) cp.execute(Arrays.asList(b("TTL"), key));
        Assert.assertEquals(-2, ttl.value());

        RespBulkString get = (RespBulkString) cp.execute(Arrays.asList(b("GET"), key));
        Assert.assertTrue(get.isNull());

        RespInteger exists = (RespInteger) cp.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

        db.shutdown();
    }

    @Test
    public void setGetIncrExpireTtl() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        Assert.assertTrue(cp.execute(cmd("SET", "a", "1")) instanceof RespSimpleString);
        RespObject get = cp.execute(cmd("GET", "a"));
        Assert.assertEquals("1", ((RespBulkString) get).asString());

        RespObject incr = cp.execute(cmd("INCR", "a"));
        Assert.assertEquals(2, ((RespInteger) incr).value());

        RespObject expire = cp.execute(cmd("EXPIRE", "a", "10"));
        Assert.assertEquals(1, ((RespInteger) expire).value());

        RespObject ttl = cp.execute(cmd("TTL", "a"));
        Assert.assertTrue(((RespInteger) ttl).value() >= 0);

        db.shutdown();
    }

    @Test
    public void commandParsingIsLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            YierdisDb db = new YierdisDb();
            CommandProcessor cp = new CommandProcessor(db);

            RespObject pong = cp.execute(cmd("ping"));
            Assert.assertTrue(pong instanceof RespSimpleString);
            Assert.assertEquals("PONG", ((RespSimpleString) pong).value());

            cp.execute(cmd("set", "a", "1"));
            RespObject type = cp.execute(cmd("type", "a"));
            Assert.assertTrue(type instanceof RespSimpleString);
            Assert.assertEquals("string", ((RespSimpleString) type).value());

            db.shutdown();
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void setNxReturnsNilWhenKeyExists() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        Assert.assertTrue(cp.execute(cmd("SET", "k", "v")) instanceof RespSimpleString);
        RespObject res = cp.execute(cmd("SET", "k", "v2", "NX"));
        Assert.assertTrue(res instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) res).isNull());

        db.shutdown();
    }

    @Test
    public void wrongTypeReturnsError() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        cp.execute(cmd("SET", "a", "1"));
        RespObject err = cp.execute(cmd("LPUSH", "a", "x"));
        Assert.assertTrue(err instanceof RespError);
        Assert.assertTrue(((RespError) err).message().startsWith("WRONGTYPE"));

        db.shutdown();
    }

    private static void assertBinarySafeRoundTrip(CommandProcessor cp, byte[] key, byte[] value) {
        Assert.assertTrue(cp.execute(Arrays.asList(b("SET"), key, value)) instanceof RespSimpleString);

        RespBulkString get = (RespBulkString) cp.execute(Arrays.asList(b("GET"), key));
        Assert.assertArrayEquals(value, get.data());

        RespInteger len = (RespInteger) cp.execute(Arrays.asList(b("STRLEN"), key));
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
