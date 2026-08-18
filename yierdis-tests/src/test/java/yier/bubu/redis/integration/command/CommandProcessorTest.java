package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.api.SlowCommandLimits;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandProcessorTest {
    @Test
    public void keysReportsAnErrorWhenItsSafetyLimitWouldTruncateTheResult() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcherWithSlowLimits(
                    db,
                    new SlowCommandLimits(0L, 1)
            );
            {
                FastTestClient client = new FastTestClient(dispatcher);
                for (int index = 0; index < 3; index++) {
                    Assert.assertTrue(client.execute(cmd("SET", "key-" + index, "value"))
                            instanceof ReplySimpleString);
                }

                ReplyError error = (ReplyError) client.execute(cmd("KEYS", "*"));

                Assert.assertEquals("ERR KEYS scan incomplete; use SCAN", error.message());
            }
        });
    }

    @Test
    public void clientMetadataCommandsAreAccepted() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                ReplySimpleString setinfo = (ReplySimpleString) client.execute(cmd("CLIENT", "SETINFO", "LIB-NAME", "go-redis"));
                Assert.assertEquals("OK", setinfo.value());

                Assert.assertTrue(client.execute(cmd("CLIENT", "GETNAME")) instanceof ReplyNull);

                ReplySimpleString setname = (ReplySimpleString) client.execute(cmd("CLIENT", "SETNAME", "test"));
                Assert.assertEquals("OK", setname.value());

                ReplyBulkString getname = (ReplyBulkString) client.execute(cmd("CLIENT", "GETNAME"));
                Assert.assertEquals("test", getname.asString());
            }
        });
    }

    @Test
    public void authReportsNoPasswordConfigured() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                ReplyError auth = (ReplyError) client.execute(cmd("AUTH", "secret"));

                Assert.assertEquals(
                        "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
                        auth.message()
                );
            }
        });
    }

    @Test
    public void stringIsBinarySafe() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("k");
            byte[] value = new byte[]{0, (byte) 0xFF, 'a', '\n'};

            Assert.assertTrue(client.execute(Arrays.asList(
                    b("SET"),
                    key,
                    value
            )) instanceof ReplySimpleString);

            ReplyObject get = client.execute(Arrays.asList(
                    b("GET"),
                    key
            ));
            Assert.assertTrue(get instanceof ReplyBulkString);
            Assert.assertArrayEquals(value, ((ReplyBulkString) get).data());

            ReplyInteger len = (ReplyInteger) client.execute(Arrays.asList(
                    b("STRLEN"),
                    key
            ));
            Assert.assertEquals(value.length, len.value());

            byte[] extra = new byte[]{1, 2, 3};
            ReplyInteger newLen = (ReplyInteger) client.execute(Arrays.asList(
                    b("APPEND"),
                    key,
                    extra
            ));
            Assert.assertEquals(value.length + extra.length, newLen.value());

            ReplyObject get2 = client.execute(Arrays.asList(
                    b("GET"),
                    key
            ));
            Assert.assertTrue(get2 instanceof ReplyBulkString);
            Assert.assertArrayEquals(new byte[]{0, (byte) 0xFF, 'a', '\n', 1, 2, 3}, ((ReplyBulkString) get2).data());
            }
        });
    }

    @Test
    public void incrWorksAfterAppendWhenRawStringHasSpareCapacity() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            Assert.assertTrue(client.execute(cmd("SET", "k", "1")) instanceof ReplySimpleString);

            ReplyInteger len = (ReplyInteger) client.execute(Arrays.asList(b("APPEND"), b("k"), b("0")));
            Assert.assertEquals(2, len.value());

            ReplyInteger incr = (ReplyInteger) client.execute(Arrays.asList(b("INCR"), b("k")));
            Assert.assertEquals(11, incr.value());

            ReplyBulkString get = (ReplyBulkString) client.execute(Arrays.asList(b("GET"), b("k")));
            Assert.assertEquals("11", get.asString());
            }
        });
    }

    @Test
    public void integerLikeStringsAreBinarySafe() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            assertBinarySafeRoundTrip(client, b("k:01"), b("01"));
            assertBinarySafeRoundTrip(client, b("k:+1"), b("+1"));
            assertBinarySafeRoundTrip(client, b("k:-0"), b("-0"));
            }
        });
    }

    @Test
    public void binaryKeyIsSupportedEndToEnd() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
            byte[] value = new byte[]{1, 2, 3};

            Assert.assertTrue(client.execute(Arrays.asList(b("SET"), key, value)) instanceof ReplySimpleString);

            ReplyBulkString get = (ReplyBulkString) client.execute(Arrays.asList(b("GET"), key));
            Assert.assertArrayEquals(value, get.data());

            ReplyInteger exists1 = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(1, exists1.value());

            ReplyArray keys = (ReplyArray) client.execute(Arrays.asList(b("KEYS"), b("*")));
            Assert.assertTrue(containsBytes(keys, key));

            ReplyInteger del = (ReplyInteger) client.execute(Arrays.asList(b("DEL"), key));
            Assert.assertEquals(1, del.value());

            ReplyInteger exists0 = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists0.value());
            }
        });
    }

    @Test
    public void keysGlobMatchesOnRawBytes() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] v = new byte[]{1};
            byte[] k1 = new byte[]{0, 'a'};
            byte[] k2 = new byte[]{0, 'b'};
            byte[] k3 = new byte[]{1, 'a'};

            client.execute(Arrays.asList(b("SET"), k1, v));
            client.execute(Arrays.asList(b("SET"), k2, v));
            client.execute(Arrays.asList(b("SET"), k3, v));

            // Prefix match: 0x00 + '*'
            ReplyArray prefix = (ReplyArray) client.execute(Arrays.asList(
                    b("KEYS"),
                    new byte[]{0, '*'}
            ));
            Assert.assertEquals(2, prefix.values().size());
            Assert.assertTrue(containsBytes(prefix, k1));
            Assert.assertTrue(containsBytes(prefix, k2));

            // Exactly 2 bytes: 0x00 + '?'
            ReplyArray oneByte = (ReplyArray) client.execute(Arrays.asList(
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
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

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

            ReplyArray ab = (ReplyArray) client.execute(cmd("KEYS", "[ab]*"));
            Assert.assertEquals(2, ab.values().size());
            Assert.assertTrue(containsBytes(ab, ka1));
            Assert.assertTrue(containsBytes(ab, kb1));

            ReplyArray aToC = (ReplyArray) client.execute(cmd("KEYS", "[a-c]*"));
            Assert.assertEquals(3, aToC.values().size());
            Assert.assertTrue(containsBytes(aToC, ka1));
            Assert.assertTrue(containsBytes(aToC, kb1));
            Assert.assertTrue(containsBytes(aToC, kc1));

            ReplyArray notA = (ReplyArray) client.execute(cmd("KEYS", "[^a]*"));
            Assert.assertTrue(containsBytes(notA, kb1));
            Assert.assertTrue(containsBytes(notA, kc1));
            Assert.assertFalse(containsBytes(notA, ka1));

            // Escapes: "\*" "\?" "\[" should match literal '*', '?', '['.
            ReplyArray stars = (ReplyArray) client.execute(cmd("KEYS", "\\*"));
            Assert.assertEquals(1, stars.values().size());
            Assert.assertTrue(containsBytes(stars, kStar));

            ReplyArray qs = (ReplyArray) client.execute(cmd("KEYS", "\\?"));
            Assert.assertEquals(1, qs.values().size());
            Assert.assertTrue(containsBytes(qs, kQ));

            ReplyArray lbrackets = (ReplyArray) client.execute(cmd("KEYS", "\\["));
            Assert.assertEquals(1, lbrackets.values().size());
            Assert.assertTrue(containsBytes(lbrackets, kLbracket));

            // Unclosed character classes are treated as literal '['.
            ReplyArray literalLbracket = (ReplyArray) client.execute(cmd("KEYS", "["));
            Assert.assertEquals(1, literalLbracket.values().size());
            Assert.assertTrue(containsBytes(literalLbracket, kLbracket));
            }
        });
    }

    @Test
    public void incrErrorsOnNonIntegerOrOverflow() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("k");

            // Non-integer
            client.execute(Arrays.asList(b("SET"), key, new byte[]{'x'}));
            ReplyObject err1 = client.execute(Arrays.asList(b("INCR"), key));
            Assert.assertTrue(err1 instanceof ReplyError);
            Assert.assertEquals("ERR value is not an integer or out of range", ((ReplyError) err1).message());

            // Overflow
            client.execute(Arrays.asList(b("SET"), key, b(Long.toString(Long.MAX_VALUE))));
            ReplyObject err2 = client.execute(Arrays.asList(b("INCR"), key));
            Assert.assertTrue(err2 instanceof ReplyError);
            Assert.assertEquals("ERR value is not an integer or out of range", ((ReplyError) err2).message());

            }
        });
    }

    @Test
    public void expireZeroDeletesKeyImmediately() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = new byte[]{0, (byte) 0xFF};
            byte[] value = new byte[]{1, 2, 3};

            client.execute(Arrays.asList(b("SET"), key, value));

            ReplyInteger expire = (ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")));
            Assert.assertEquals(1, expire.value());

            ReplyInteger ttl = (ReplyInteger) client.execute(Arrays.asList(b("TTL"), key));
            Assert.assertEquals(-2, ttl.value());

	            ReplyObject get = client.execute(Arrays.asList(b("GET"), key));
	            Assert.assertTrue(get instanceof ReplyNull);

            ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            }
        });
    }

    @Test
    public void setGetIncrExpireTtl() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            Assert.assertTrue(client.execute(cmd("SET", "a", "1")) instanceof ReplySimpleString);
            ReplyObject get = client.execute(cmd("GET", "a"));
            Assert.assertEquals("1", ((ReplyBulkString) get).asString());

            ReplyObject incr = client.execute(cmd("INCR", "a"));
            Assert.assertEquals(2, ((ReplyInteger) incr).value());

            ReplyObject expire = client.execute(cmd("EXPIRE", "a", "10"));
            Assert.assertEquals(1, ((ReplyInteger) expire).value());

            ReplyObject ttl = client.execute(cmd("TTL", "a"));
            Assert.assertTrue(((ReplyInteger) ttl).value() >= 0);

            }
        });
    }

    @Test
    public void commandParsingIsLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            forEachDb(db -> {
                CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
                {
                    FastTestClient client = new FastTestClient(dispatcher);

                ReplyObject pong = client.execute(cmd("ping"));
                Assert.assertTrue(pong instanceof ReplySimpleString);
                Assert.assertEquals("PONG", ((ReplySimpleString) pong).value());

                client.execute(cmd("set", "a", "1"));
                ReplyObject type = client.execute(cmd("type", "a"));
                Assert.assertTrue(type instanceof ReplySimpleString);
                Assert.assertEquals("string", ((ReplySimpleString) type).value());
                }
            });
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void setNxReturnsNilWhenKeyExists() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

	            Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
	            ReplyObject res = client.execute(cmd("SET", "k", "v2", "NX"));
	            Assert.assertTrue(res instanceof ReplyNull);

	            }
	        });
	    }

    @Test
    public void setGetAndKeepTtlSemanticsRemainIntact() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                Assert.assertTrue(client.execute(cmd("SET", "k", "v", "EX", "5")) instanceof ReplySimpleString);
                Assert.assertTrue(((ReplyInteger) client.execute(cmd("TTL", "k"))).value() > 0L);

                ReplyBulkString old = (ReplyBulkString) client.execute(cmd("SET", "k", "v2", "KEEPTTL", "GET"));
                Assert.assertEquals("v", old.asString());
                Assert.assertTrue(((ReplyInteger) client.execute(cmd("TTL", "k"))).value() > 0L);

                ReplyObject nxGet = client.execute(cmd("SET", "k", "v3", "NX", "GET"));
                Assert.assertTrue(nxGet instanceof ReplyBulkString);
                Assert.assertEquals("v2", ((ReplyBulkString) nxGet).asString());
            }
        });
    }

    @Test
    public void setGetOnNonStringKeyReturnsWrongType() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                ReplyObject push = client.execute(cmd("LPUSH", "k", "x"));
                Assert.assertTrue(push instanceof ReplyInteger);

                ReplyObject res = client.execute(cmd("SET", "k", "v", "GET"));
                Assert.assertTrue(res instanceof ReplyError);
                Assert.assertTrue(((ReplyError) res).message().startsWith("WRONGTYPE"));
            }
        });
    }

    @Test
    public void wrongTypeReturnsError() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            client.execute(cmd("SET", "a", "1"));
            ReplyObject err = client.execute(cmd("LPUSH", "a", "x"));
            Assert.assertTrue(err instanceof ReplyError);
            Assert.assertTrue(((ReplyError) err).message().startsWith("WRONGTYPE"));

            }
        });
    }

    private static void assertBinarySafeRoundTrip(FastTestClient client, byte[] key, byte[] value) {
        Assert.assertTrue(client.execute(Arrays.asList(b("SET"), key, value)) instanceof ReplySimpleString);

        ReplyBulkString get = (ReplyBulkString) client.execute(Arrays.asList(b("GET"), key));
        Assert.assertArrayEquals(value, get.data());

        ReplyInteger len = (ReplyInteger) client.execute(Arrays.asList(b("STRLEN"), key));
        Assert.assertEquals(value.length, len.value());
    }

    private static boolean containsBytes(ReplyArray array, byte[] expected) {
        for (ReplyObject o : array.values()) {
            if (o instanceof ReplyBulkString && Arrays.equals(expected, ((ReplyBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }
}
