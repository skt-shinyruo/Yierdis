package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.ReplyAssertions.assertArraySize;
import static yier.bubu.redis.testutil.ReplyAssertions.assertBulkString;
import static yier.bubu.redis.testutil.ReplyAssertions.assertErrorContaining;
import static yier.bubu.redis.testutil.ReplyAssertions.assertInteger;
import static yier.bubu.redis.testutil.ReplyAssertions.assertNull;
import static yier.bubu.redis.testutil.ReplyAssertions.assertSimpleString;
import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class StringCommandTest {
    @Test
    public void stringCommandsCoverBinarySafeSetGetStrlenAndAppend() {
        withClient(client -> {
            byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
            byte[] value = new byte[]{0, (byte) 0xFE, 'v'};

            assertSimpleString("OK", client.execute(Arrays.asList(b("SET"), key, value)));
            assertBulkString(value, client.execute(Arrays.asList(b("GET"), key)));
            assertInteger(value.length, client.execute(Arrays.asList(b("STRLEN"), key)));

            assertInteger(value.length + 2, client.execute(Arrays.asList(b("APPEND"), key, new byte[]{1, 2})));
            assertBulkString(new byte[]{0, (byte) 0xFE, 'v', 1, 2}, client.execute(Arrays.asList(b("GET"), key)));
        });
    }

    @Test
    public void setOptionsCoverNxXxGetAndTtlModes() {
        withClient(client -> {
            assertNull(client.execute(cmd("SET", "mode", "ignored", "XX")));
            assertSimpleString("OK", client.execute(cmd("SET", "mode", "v1", "NX")));
            assertNull(client.execute(cmd("SET", "mode", "ignored", "NX")));
            assertBulkString("v1", client.execute(cmd("SET", "mode", "v2", "XX", "GET")));
            assertBulkString("v2", client.execute(cmd("GET", "mode")));
            assertNull(client.execute(cmd("SET", "new", "v", "GET")));
            assertBulkString("v", client.execute(cmd("GET", "new")));

            assertSimpleString("OK", client.execute(cmd("SET", "expire", "v", "EX", "60")));
            assertPositiveInteger(client.execute(cmd("TTL", "expire")));
            assertBulkString("v", client.execute(cmd("SET", "expire", "v2", "KEEPTTL", "GET")));
            assertPositiveInteger(client.execute(cmd("TTL", "expire")));
            assertSimpleString("OK", client.execute(cmd("SET", "px", "v", "PX", "60000")));
            assertPositiveInteger(client.execute(cmd("PTTL", "px")));
            long exat = (System.currentTimeMillis() / 1000L) + 60L;
            assertSimpleString("OK", client.execute(cmd("SET", "exat", "v", "EXAT", Long.toString(exat))));
            assertPositiveInteger(client.execute(cmd("TTL", "exat")));
            long pxat = System.currentTimeMillis() + 60_000L;
            assertSimpleString("OK", client.execute(cmd("SET", "pxat", "v", "PXAT", Long.toString(pxat))));
            assertPositiveInteger(client.execute(cmd("PTTL", "pxat")));
        });
    }

    @Test
    public void setNxGetReturnsTheExistingStringWithoutReplacingIt() {
        withClient(client -> {
            assertSimpleString("OK", client.execute(cmd("SET", "key", "old")));

            assertBulkString("old", client.execute(cmd("SET", "key", "new", "NX", "GET")));
            assertBulkString("old", client.execute(cmd("GET", "key")));
        });
    }

    @Test
    public void setNxGetReportsWrongTypeForAnExistingList() {
        withClient(client -> {
            assertInteger(1, client.execute(cmd("LPUSH", "key", "item")));

            assertErrorContaining("WRONGTYPE", client.execute(cmd("SET", "key", "new", "NX", "GET")));
            assertBulkString("item", assertArraySize(1, client.execute(cmd("LRANGE", "key", "0", "-1"))).values().get(0));
        });
    }

    @Test
    public void setWithoutGetReplacesExistingValuesRegardlessOfTheirType() {
        withClient(client -> {
            assertInteger(1, client.execute(cmd("LPUSH", "normal-key", "item")));

            assertSimpleString("OK", client.execute(cmd("SET", "normal-key", "normal")));
            assertBulkString("normal", client.execute(cmd("GET", "normal-key")));

            assertInteger(1, client.execute(cmd("LPUSH", "xx-key", "item")));
            assertSimpleString("OK", client.execute(cmd("SET", "xx-key", "conditional", "XX")));
            assertBulkString("conditional", client.execute(cmd("GET", "xx-key")));
        });
    }

    @Test
    public void counterCommandsCoverIncrDecrAndInvalidInteger() {
        withClient(client -> {
            assertInteger(1, client.execute(cmd("INCR", "counter")));
            assertInteger(0, client.execute(cmd("DECR", "counter")));
            assertSimpleString("OK", client.execute(cmd("SET", "mode", "v")));
            assertErrorContaining("not an integer", client.execute(cmd("INCR", "mode")));
        });
    }

    @Test
    public void counterCommandsRequireCanonicalIntegers() {
        withClient(client -> {
            assertSimpleString("OK", client.execute(cmd("SET", "plus", "+1")));
            assertErrorContaining("not an integer or out of range", client.execute(cmd("INCR", "plus")));
            assertBulkString("+1", client.execute(cmd("GET", "plus")));

            assertSimpleString("OK", client.execute(cmd("SET", "zeros", "007")));
            assertErrorContaining("not an integer or out of range", client.execute(cmd("INCR", "zeros")));
            assertBulkString("007", client.execute(cmd("GET", "zeros")));

            assertSimpleString("OK", client.execute(cmd("SET", "negzero", "-0")));
            assertErrorContaining("not an integer or out of range", client.execute(cmd("INCR", "negzero")));
            assertBulkString("-0", client.execute(cmd("GET", "negzero")));

            assertSimpleString("OK", client.execute(cmd("SET", "zero", "0")));
            assertInteger(1, client.execute(cmd("INCR", "zero")));
            assertSimpleString("OK", client.execute(cmd("SET", "neg", "-41")));
            assertInteger(-42, client.execute(cmd("DECR", "neg")));
        });
    }

    @Test
    public void setCommandCoversSyntaxAndExpiryErrors() {
        withClient(client -> {
            assertErrorContaining("syntax error", client.execute(cmd("SET", "k", "v", "NX", "XX")));
            assertErrorContaining("syntax error", client.execute(cmd("SET", "k", "v", "EX", "60", "KEEPTTL")));
            assertErrorContaining("invalid expire time", client.execute(cmd("SET", "k", "v", "EX", "0")));
            assertErrorContaining("not an integer or out of range", client.execute(cmd("SET", "k", "v", "PX", "abc")));
        });
    }

    @Test
    public void setTreatsRepeatedFlagsAsIdempotentLikeRedis() {
        withClient(client -> {
            assertSimpleString("OK", client.execute(cmd("SET", "k", "v1", "NX", "NX")));
            assertNull(client.execute(cmd("SET", "k", "ignored", "NX", "NX")));
            assertSimpleString("OK", client.execute(cmd("SET", "k", "v2", "XX", "XX")));
            assertBulkString("v2", client.execute(cmd("SET", "k", "v3", "GET", "GET")));
            assertSimpleString("OK", client.execute(cmd("SET", "k", "v4", "KEEPTTL", "KEEPTTL")));

            assertSimpleString("OK", client.execute(cmd("SET", "dup-expire", "v", "EX", "60", "EX", "70")));
            long pttl = ((ReplyInteger) client.execute(cmd("PTTL", "dup-expire"))).value();
            Assert.assertTrue("last repeated EX wins, pttl=" + pttl, pttl > 60_000L && pttl <= 70_000L);

            assertErrorContaining("syntax error", client.execute(cmd("SET", "k", "v", "NX", "XX")));
            assertErrorContaining("syntax error", client.execute(cmd("SET", "k", "v", "XX", "NX")));
            assertErrorContaining("syntax error", client.execute(cmd("SET", "k", "v", "EX", "60", "PX", "60000")));
            assertErrorContaining("syntax error", client.execute(cmd("SET", "k", "v", "KEEPTTL", "EX", "60")));
        });
    }

    private static void withClient(ClientCase test) {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                test.run(client);
            }
        });
    }

    private static void assertPositiveInteger(ReplyObject reply) {
        Assert.assertTrue("expected integer reply", reply instanceof ReplyInteger);
        long value = ((ReplyInteger) reply).value();
        Assert.assertTrue("expected positive integer but got " + value, value > 0L);
    }

    @FunctionalInterface
    private interface ClientCase {
        void run(FastTestClient client);
    }
}
