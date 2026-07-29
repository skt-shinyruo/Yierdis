package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyNullArray;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandVariantCoverageTest {
    @Test
    public void connectionCommandsCoverPingEchoQuitAndSelectValidation() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplySimpleString ping = (ReplySimpleString) client.execute(cmd("PING"));
                Assert.assertEquals("PONG", ping.value());

                ReplyBulkString pingPayload = (ReplyBulkString) client.execute(cmd("PING", "payload"));
                Assert.assertEquals("payload", pingPayload.asString());

                byte[] echoPayload = new byte[]{0, (byte) 0xFE, 'e'};
                ReplyBulkString echo = (ReplyBulkString) client.execute(List.of(b("ECHO"), echoPayload));
                Assert.assertArrayEquals(echoPayload, echo.data());

                ReplySimpleString select = (ReplySimpleString) client.execute(cmd("SELECT", "0"));
                Assert.assertEquals("OK", select.value());

                ReplyError badSelect = (ReplyError) client.execute(cmd("SELECT", "not-int"));
                Assert.assertEquals("ERR value is not an integer or out of range", badSelect.message());

                ReplySimpleString quit = (ReplySimpleString) client.execute(cmd("QUIT"));
                Assert.assertEquals("OK", quit.value());
            }
        });
    }

    @Test
    public void connectionCommandsAcceptNullBulkMessages() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertTrue(client.execute(java.util.Arrays.asList(b("PING"), null)) instanceof ReplyNull);
                Assert.assertTrue(client.execute(java.util.Arrays.asList(b("ECHO"), null)) instanceof ReplyNull);
            }
        });
    }

    @Test
    public void quitRequestsCloseAfterReply() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            boolean[] closeAfterReply = new boolean[1];
            yier.bubu.redis.execution.api.TransactionState transaction =
                    (yier.bubu.redis.execution.api.TransactionState) java.lang.reflect.Proxy.newProxyInstance(
                            yier.bubu.redis.execution.api.TransactionState.class.getClassLoader(),
                            new Class<?>[]{yier.bubu.redis.execution.api.TransactionState.class},
                            (proxy, method, arguments) -> method.getName().equals("active") ? false : null
                    );
            CommandSession session = (CommandSession) java.lang.reflect.Proxy.newProxyInstance(
                    CommandSession.class.getClassLoader(),
                    new Class<?>[]{CommandSession.class},
                    (proxy, method, arguments) -> method.getName().equals("transaction") ? transaction : null
            );
            RedisReplyWriter writer = (RedisReplyWriter) java.lang.reflect.Proxy.newProxyInstance(
                    RedisReplyWriter.class.getClassLoader(),
                    new Class<?>[]{RedisReplyWriter.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("requestCloseAfterReply")) {
                            closeAfterReply[0] = true;
                        }
                        if (method.getName().equals("closeAfterReplyRequested")) {
                            return closeAfterReply[0];
                        }
                        return null;
                    }
            );
            try (ByteArrayExecutionRequest request = ByteArrayExecutionRequest.copyOf(cmd("QUIT"));
                    PreparedCommand prepared = dispatcher.prepare(session, request);
                    CommandExecutionContext execution = CommandExecutionContext.forRequest(session, writer, request)) {
                Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
                prepared.execute(execution);
                Assert.assertTrue(closeAfterReply[0]);
            }
        });
    }

    @Test
    public void commandVariantsCoverBaseCountInfoAndUnknownName() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyArray base = (ReplyArray) client.execute(cmd("COMMAND"));
                Assert.assertFalse(base.values().isEmpty());

                ReplyInteger count = (ReplyInteger) client.execute(cmd("COMMAND", "COUNT"));
                Assert.assertTrue(count.value() >= base.values().size());

                ReplyArray info = (ReplyArray) client.execute(cmd("COMMAND", "INFO", "GET", "NO_SUCH_COMMAND"));
                Assert.assertEquals(2, info.values().size());
                assertCommandInfo(info.values().get(0), "get");
                Assert.assertTrue(info.values().get(1) instanceof ReplyNullArray);

                ReplyArray emptyName = (ReplyArray) client.execute(List.of(b("COMMAND"), b("INFO"), b("")));
                Assert.assertEquals(1, emptyName.values().size());
                Assert.assertTrue(emptyName.values().get(0) instanceof ReplyNullArray);
            }
        });
    }

    @Test
    public void clientUnknownSubcommandReturnsRedisStyleError() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyError error = (ReplyError) client.execute(cmd("CLIENT", "BOGUS"));
                Assert.assertEquals("ERR unknown subcommand 'BOGUS'. Try CLIENT HELP.", error.message());
            }
        });
    }

    @Test
    public void scanVariantsCoverInvalidCursorAndDuplicateOptions() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertTrue(client.execute(cmd("SET", "scan:1", "v")) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(cmd("SET", "scan:2", "v")) instanceof ReplySimpleString);

                ReplyError invalidCursor = (ReplyError) client.execute(cmd("SCAN", "-1"));
                Assert.assertEquals("ERR value is not an integer or out of range", invalidCursor.message());

                ReplyArray duplicateMatch = (ReplyArray) client.execute(cmd(
                        "SCAN", "0", "MATCH", "scan:*", "MATCH", "scan:1", "COUNT", "2", "COUNT", "1"
                ));
                Assert.assertEquals(2, duplicateMatch.values().size());
                ReplyArray keys = (ReplyArray) duplicateMatch.values().get(1);
                Assert.assertEquals(1, keys.values().size());
                Assert.assertEquals("scan:1", ((ReplyBulkString) keys.values().get(0)).asString());
            }
        });
    }

    @Test
    public void setVariantsCoverXxPxExatPxatAndConflicts() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertTrue(client.execute(cmd("SET", "k", "v", "XX")) instanceof ReplyNull);

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd("SET", "k", "v1", "PX", "60000"))).value());
                Assert.assertTrue(((ReplyInteger) client.execute(cmd("PTTL", "k"))).value() > 0L);

                long exat = (System.currentTimeMillis() / 1000L) + 60L;
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd(
                        "SET", "k", "v2", "XX", "EXAT", Long.toString(exat)
                ))).value());
                Assert.assertTrue(((ReplyInteger) client.execute(cmd("TTL", "k"))).value() > 0L);

                long pxat = System.currentTimeMillis() + 60_000L;
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd(
                        "SET", "k2", "v3", "PXAT", Long.toString(pxat)
                ))).value());
                Assert.assertTrue(((ReplyInteger) client.execute(cmd("PTTL", "k2"))).value() > 0L);

                ReplyError conflict = (ReplyError) client.execute(cmd("SET", "k", "v", "EX", "60", "KEEPTTL"));
                Assert.assertEquals("ERR syntax error", conflict.message());
            }
        });
    }

    @Test
    public void bitcountInvalidBoundsRejectNonIntegerRanges() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyError error = (ReplyError) client.execute(cmd("BITCOUNT", "k", "zero", "1"));
                Assert.assertEquals("ERR value is not an integer or out of range", error.message());
            }
        });
    }

    @Test
    public void rpopCountVariantsCoverNullArrayEmptyArrayAndNegativeCount() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertTrue(client.execute(cmd("RPOP", "missing")) instanceof ReplyNull);
                Assert.assertTrue(client.execute(cmd("RPOP", "missing", "2")) instanceof ReplyNullArray);

                ReplyArray zeroCount = (ReplyArray) client.execute(cmd("RPOP", "missing", "0"));
                Assert.assertEquals(List.of(), zeroCount.values());

                ReplyError negative = (ReplyError) client.execute(cmd("RPOP", "missing", "-1"));
                Assert.assertEquals("ERR value is not an integer or out of range", negative.message());
            }
        });
    }

    @Test
    public void zrevrangeInvalidOptionIsSyntaxError() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyError error = (ReplyError) client.execute(cmd("ZREVRANGE", "z", "0", "-1", "BAD"));
                Assert.assertEquals("ERR syntax error", error.message());
            }
        });
    }

    @Test
    public void flushdbVariantsCoverDefaultSyncAsyncAndInvalidMode() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd("FLUSHDB"))).value());
                Assert.assertTrue(client.execute(cmd("GET", "k")) instanceof ReplyNull);

                Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd("FLUSHDB", "SYNC"))).value());
                Assert.assertTrue(client.execute(cmd("GET", "k")) instanceof ReplyNull);

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd("FLUSHDB", "ASYNC"))).value());

                ReplyError invalid = (ReplyError) client.execute(cmd("FLUSHDB", "BAD"));
                Assert.assertEquals("ERR syntax error", invalid.message());
            }
        });
    }

    private static void assertCommandInfo(ReplyObject obj, String expectedName) {
        ReplyArray fields = (ReplyArray) obj;
        Assert.assertEquals(6, fields.values().size());
        Assert.assertEquals(expectedName, ((ReplyBulkString) fields.values().get(0)).asString());
    }
}
