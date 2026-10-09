package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class TransactionCommandTest {
    private record InvalidCommand(List<byte[]> args, String message) {
    }

    @Test
    public void multiQueuesAndExecAppliesInOrder() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());
                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("k"), b("v")))).value());
                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(Arrays.asList(b("GET"), b("k")))).value());

                ReplyArray exec = (ReplyArray) client.execute(Arrays.asList(b("EXEC")));
                Assert.assertNotNull(exec.values());
                Assert.assertEquals(2, exec.values().size());
                Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(0)).value());
                Assert.assertEquals("v", ((ReplyBulkString) exec.values().get(1)).asString());

                Assert.assertEquals("v", ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), b("k")))).asString());
            }
        });
    }

    @Test
    public void execFreezesKeysAndScanAtTheMomentTheyRun() {
        forEachDb(db -> {
            keysThenInsertSeesOnlyTheEarlierKey(db);
            keysWithStableCountDoesNotMixLaterMembers(db);
            scanWithStableCountDoesNotMixLaterMembers(db);
            keysThenManyInsertsKeepsTheOriginalMembers(db);
            keysPatternDoesNotIncludeALaterMatch(db);
        });
    }

    private static void keysThenInsertSeesOnlyTheEarlierKey(YierdisDb db) {
        FastTestClient client = client(db);
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("a"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("KEYS"), b("*")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SET"), b("b"), b("1")))).value());

        ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
        Assert.assertEquals(2, exec.values().size());
        Assert.assertEquals(Set.of("a"), bulkStrings((ReplyArray) exec.values().get(0)));
        Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(1)).value());
        Assert.assertEquals(Set.of("a", "b"), bulkStrings((ReplyArray) client.execute(List.of(b("KEYS"), b("*")))));
    }

    private static void keysWithStableCountDoesNotMixLaterMembers(YierdisDb db) {
        FastTestClient client = client(db);
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("h"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("a"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("c"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("KEYS"), b("*")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("DEL"), b("a")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SET"), b("b"), b("1")))).value());

        ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
        Assert.assertEquals(3, exec.values().size());
        Assert.assertEquals(Set.of("h", "a", "c"), bulkStrings((ReplyArray) exec.values().get(0)));
        Assert.assertEquals(1L, ((ReplyInteger) exec.values().get(1)).value());
        Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(2)).value());
    }

    private static void scanWithStableCountDoesNotMixLaterMembers(YierdisDb db) {
        FastTestClient client = client(db);
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("h"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("a"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("c"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SCAN"), b("0")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("DEL"), b("a")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SET"), b("b"), b("1")))).value());

        ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
        ReplyArray scan = (ReplyArray) exec.values().get(0);
        Assert.assertEquals(Set.of("h", "a", "c"), bulkStrings((ReplyArray) scan.values().get(1)));
        Assert.assertEquals(1L, ((ReplyInteger) exec.values().get(1)).value());
    }

    private static void keysThenManyInsertsKeepsTheOriginalMembers(YierdisDb db) {
        FastTestClient client = client(db);
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("a"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("c"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("h"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("KEYS"), b("*")))).value());
        for (int index = 1; index <= 20; index++) {
            Assert.assertEquals(
                    "QUEUED",
                    ((ReplySimpleString) client.execute(List.of(b("SET"), b("k" + index), b("1")))).value()
            );
        }

        ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
        Assert.assertEquals(21, exec.values().size());
        Assert.assertEquals(Set.of("a", "c", "h"), bulkStrings((ReplyArray) exec.values().get(0)));
        for (int index = 1; index <= 20; index++) {
            Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(index)).value());
        }
    }

    private static void keysPatternDoesNotIncludeALaterMatch(YierdisDb db) {
        FastTestClient client = client(db);
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("SET"), b("h"), b("1")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("KEYS"), b("h*")))).value());
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SET"), b("h2"), b("1")))).value());

        ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
        Assert.assertEquals(Set.of("h"), bulkStrings((ReplyArray) exec.values().get(0)));
        Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(1)).value());
        Assert.assertEquals(1L, ((ReplyInteger) client.execute(List.of(b("EXISTS"), b("h2")))).value());
    }

    private static FastTestClient client(YierdisDb db) {
        FastTestClient client = new FastTestClient(TestCommandComposition.createDispatcher(db), new TestSession());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("FLUSHDB")))).value());
        return client;
    }

    private static Set<String> bulkStrings(ReplyArray array) {
        Set<String> names = new HashSet<>();
        for (ReplyObject value : array.values()) {
            names.add(((ReplyBulkString) value).asString());
        }
        return names;
    }

    @Test
    public void execKeepsStreamedChildAliveUntilTheAggregateIsRendered() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
                List<byte[]> push = List.of(
                        b("RPUSH"), b("streamed:list"), b("one"), b("two"));
                ReplySimpleString pushQueued = (ReplySimpleString) client.execute(push);
                Assert.assertEquals("QUEUED", pushQueued.value());
                List<byte[]> rangeRequest = List.of(
                        b("LRANGE"), b("streamed:list"), b("0"), b("-1"));
                ReplySimpleString rangeQueued = (ReplySimpleString) client.execute(rangeRequest);
                Assert.assertEquals("QUEUED", rangeQueued.value());

                ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
                Assert.assertEquals(2L, ((ReplyInteger) exec.values().get(0)).value());
                ReplyArray range = (ReplyArray) exec.values().get(1);
                Assert.assertEquals("one", ((ReplyBulkString) range.values().get(0)).asString());
                Assert.assertEquals("two", ((ReplyBulkString) range.values().get(1)).asString());
            }
        });
    }

    @Test
    public void execAndDiscardWithoutMultiReturnErrors() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("ERR EXEC without MULTI", ((ReplyError) exec).message());

                ReplyObject discard = client.execute(Arrays.asList(b("DISCARD")));
                Assert.assertTrue(discard instanceof ReplyError);
                Assert.assertEquals("ERR DISCARD without MULTI", ((ReplyError) discard).message());
            }
        });
    }

    @Test
    public void multiCannotBeNested() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject nested = client.execute(Arrays.asList(b("MULTI")));
                Assert.assertTrue(nested instanceof ReplyError);
                Assert.assertEquals("ERR MULTI calls can not be nested", ((ReplyError) nested).message());

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("DISCARD")))).value());
            }
        });
    }

    @Test
    public void multiQueueCopiesArgvToPreventMutationAfterEnqueue() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                byte[] key = b("k");
                byte[] value = b("v1");
                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), key, value))).value());

                ExecutionRequest queued = session.transactionState().queued(0);
                Assert.assertArrayEquals(b("SET"), queued.toByteArray(0));
                Assert.assertArrayEquals(b("k"), queued.toByteArray(1));
                Assert.assertArrayEquals(b("v1"), queued.toByteArray(2));

                // Mutate the original value buffer after it was enqueued.
                value[1] = (byte) '2';
                Assert.assertArrayEquals(b("v1"), queued.toByteArray(2));

                ReplyArray exec = (ReplyArray) client.execute(Arrays.asList(b("EXEC")));
                Assert.assertNotNull(exec.values());
                Assert.assertEquals(1, exec.values().size());
                Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(0)).value());

                Assert.assertEquals("v1", ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), key))).asString());
            }
        });
    }

    @Test
    public void modulesCanRejectCommandsInsideMultiAndAbortTransaction() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(
                    db,
                    registration -> registration.register(new CommandSpec(
                            new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                                    TransactionPolicy.DISALLOWED_IN_MULTI),
                            args -> session -> PreparedCommands.ready(RedisReplies.simpleString("HELLO"))
                    ))
            );
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject hello = client.execute(Arrays.asList(b("HELLO")));
                Assert.assertTrue(hello instanceof ReplyError);
                Assert.assertEquals("ERR HELLO is not allowed in MULTI", ((ReplyError) hello).message());

                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
            }
        });
    }

    @Test
    public void syntaxErrorInsideMultiAbortsBeforeExec() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(
                    db,
                    registration -> registration.register(new CommandSpec(
                            new CommandSyntax("STRICT", CommandArity.exact(2), CommandKeySpec.NONE,
                                    TransactionPolicy.QUEUEABLE),
                            args -> session -> PreparedCommands.ready(RedisReplies.simpleString("OK"))
                    ))
            );
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject wrongArity = client.execute(Arrays.asList(b("STRICT")));
                Assert.assertTrue(wrongArity instanceof ReplyError);
                Assert.assertEquals("ERR wrong number of arguments for 'strict' command", ((ReplyError) wrongArity).message());
                Assert.assertEquals(0, session.transactionState().size());

                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
            }
        });
    }

    @Test
    public void unknownCommandInsideMultiAbortsBeforeExec() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject unknown = client.execute(Arrays.asList(b("NO_SUCH_COMMAND")));
                Assert.assertTrue(unknown instanceof ReplyError);
                Assert.assertEquals("ERR unknown command 'NO_SUCH_COMMAND'", ((ReplyError) unknown).message());
                Assert.assertEquals(0, session.transactionState().size());

                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
            }
        });
    }

    @Test
    public void builtInWrongArityInsideMultiAbortsBeforeExecAfterParserMigration() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject wrongArity = client.execute(Arrays.asList(b("GET")));
                Assert.assertTrue(wrongArity instanceof ReplyError);
                Assert.assertEquals("ERR wrong number of arguments for 'get' command", ((ReplyError) wrongArity).message());
                Assert.assertEquals(0, session.transactionState().size());

                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
            }
        });
    }

    @Test
    public void setOptionSyntaxInsideMultiFailsOnlyThatCommand() {
        forEachDb(db -> assertContentErrorFailsOnlyThatCommand(
                db,
                Arrays.asList(b("SET"), b("k"), b("v"), b("NX"), b("XX")),
                "ERR syntax error"
        ));
    }

    @Test
    public void representativeQueueTimeErrorsLeaveEarlierWrites() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            client.execute(List.of(b("FLUSHDB")));

            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
            Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SET"), b("a"), b("1")))).value());
            Assert.assertEquals(
                    "QUEUED",
                    ((ReplySimpleString) client.execute(List.of(b("SET"), b("b"), b("1"), b("FOO")))).value()
            );
            ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
            Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(0)).value());
            Assert.assertEquals("ERR syntax error", ((ReplyError) exec.values().get(1)).message());
            Assert.assertEquals("1", ((ReplyBulkString) client.execute(List.of(b("GET"), b("a")))).asString());

            assertContentErrorFailsOnlyThatCommand(
                    db, List.of(b("SET"), b("k"), b("0"), b("XX"), b("NX")), "ERR syntax error");
            assertContentErrorFailsOnlyThatCommand(
                    db,
                    List.of(b("ZADD"), b("k"), b("XX"), b("NX"), b("1"), b("m")),
                    "ERR XX and NX options at the same time are not compatible"
            );
            assertContentErrorFailsOnlyThatCommand(
                    db,
                    List.of(b("SETBIT"), b("k"), b("-1"), b("1")),
                    "ERR bit offset is not an integer or out of range"
            );
            assertContentErrorFailsOnlyThatCommand(
                    db,
                    List.of(b("EXPIRE"), b("k"), b("x"), b("NX")),
                    "ERR value is not an integer or out of range"
            );
            assertContentErrorFailsOnlyThatCommand(
                    db,
                    List.of(b("LPOP"), b("k"), b("-1")),
                    "ERR value is out of range, must be positive"
            );
        });
    }

    @Test
    public void bitmapParseErrorsInsideMultiFailOnlyThatCommand() {
        forEachDb(db -> assertContentErrorFailsOnlyThatCommand(
                db,
                Arrays.asList(b("SETBIT"), b("k"), b("0"), b("nope")),
                "ERR bit is not an integer or out of range"
        ));
    }

    @Test
    public void keyspaceArityErrorsInsideMultiAbortBeforeExec() {
        forEachDb(db -> {
            for (List<byte[]> invalid : List.of(
                    Arrays.asList(b("MEMORY"), b("USAGE")),
                    Arrays.asList(b("MEMORY"), b("STATS"), b("extra")),
                    Arrays.asList(b("OBJECT"), b("ENCODING"))
            )) {
                CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
                TestSession session = new TestSession();
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
                Assert.assertTrue(client.execute(invalid) instanceof ReplyError);
                Assert.assertEquals(0, session.transactionState().size());
                ReplyObject exec = client.execute(List.of(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals(
                        "EXECABORT Transaction discarded because of previous errors.",
                        ((ReplyError) exec).message()
                );
            }
        });
    }

    @Test
    public void keyspaceContentErrorsInsideMultiFailOnlyThatCommand() {
        forEachDb(db -> {
            for (List<byte[]> invalid : List.of(
                    Arrays.asList(b("SCAN"), b("-1")),
                    Arrays.asList(b("SCAN"), b("0"), b("MATCH")),
                    Arrays.asList(b("SCAN"), b("0"), b("COUNT")),
                    Arrays.asList(b("SCAN"), b("0"), b("COUNT"), b("0")),
                    Arrays.asList(b("EXPIRE"), b("k"), b("nope")),
                    Arrays.asList(b("PEXPIRE"), b("k"), b("nope")),
                    Arrays.asList(b("EXPIREAT"), b("k"), b("nope")),
                    Arrays.asList(b("PEXPIREAT"), b("k"), b("nope"))
            )) {
                assertContentErrorFailsOnlyThatCommand(db, invalid, null);
            }
        });
    }

    @Test
    public void collectionParseErrorsInsideMultiFailOnlyThatCommand() {
        forEachDb(db -> {
            for (List<byte[]> invalid : List.of(
                    Arrays.asList(b("LPOP"), b("list"), b("-1")),
                    Arrays.asList(b("RPOP"), b("list"), b("not-a-number")),
                    Arrays.asList(b("HSCAN"), b("hash"), b("-1")),
                    Arrays.asList(b("HSCAN"), b("hash"), b("0"), b("MATCH")),
                    Arrays.asList(b("HSCAN"), b("hash"), b("0"), b("COUNT"), b("0")),
                    Arrays.asList(b("SSCAN"), b("set"), b("18446744073709551616")),
                    Arrays.asList(b("SSCAN"), b("set"), b("0"), b("COUNT")),
                    Arrays.asList(b("SSCAN"), b("set"), b("0"), b("NOVALUES"))
            )) {
                assertContentErrorFailsOnlyThatCommand(db, invalid, null);
            }
        });
    }

    @Test
    public void sortedSetArityErrorsInsideMultiAbortBeforeExec() {
        forEachDb(db -> {
            for (InvalidCommand invalid : List.of(
                    invalid("ERR wrong number of arguments for 'pfcount' command", "PFCOUNT"),
                    invalid("ERR wrong number of arguments for 'pfmerge' command", "PFMERGE", "dest")
            )) {
                CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
                TestSession session = new TestSession();
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
                ReplyObject failure = client.execute(invalid.args());
                Assert.assertTrue(failure instanceof ReplyError);
                Assert.assertEquals(invalid.message(), ((ReplyError) failure).message());
                Assert.assertEquals(0, session.transactionState().size());
                ReplyObject exec = client.execute(List.of(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals(
                        "EXECABORT Transaction discarded because of previous errors.",
                        ((ReplyError) exec).message()
                );
            }
        });
    }

    @Test
    public void sortedSetAndHllContentErrorsInsideMultiFailOnlyThatCommand() {
        forEachDb(db -> {
            for (InvalidCommand invalid : List.of(
                    invalid("ERR value is not a valid float", "ZADD", "z", "NaN", "member"),
                    invalid("ERR value is not a valid float", "ZADD", "z", "bad", "member"),
                    invalid("ERR XX and NX options at the same time are not compatible",
                            "ZADD", "z", "NX", "XX", "1", "a"),
                    invalid("ERR GT, LT, and/or NX options at the same time are not compatible",
                            "ZADD", "z", "GT", "NX", "1", "a"),
                    invalid("ERR GT, LT, and/or NX options at the same time are not compatible",
                            "ZADD", "z", "GT", "LT", "1", "a"),
                    invalid("ERR INCR option supports a single increment-element pair",
                            "ZADD", "z", "INCR", "1", "a", "2", "b"),
                    invalid("ERR syntax error", "ZADD", "z", "NX", "XX"),
                    invalid("ERR value is not an integer or out of range", "ZRANGE", "z", "bad", "-1"),
                    invalid("ERR syntax error", "ZRANGE", "z", "0", "-1", "WITHSCORES", "WITHSCORES"),
                    invalid("ERR syntax error", "ZREVRANGE", "z", "0", "-1", "UNKNOWN"),
                    invalid("ERR value is not an integer or out of range",
                            "ZREVRANGE", "z", "bad", "-1", "UNKNOWN"),
                    invalid("ERR min or max is not a float", "ZRANGEBYSCORE", "z", "bad", "+inf"),
                    invalid("ERR value is not an integer or out of range",
                            "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "bad", "1"),
                    invalid("ERR min or max is not a float", "ZREMRANGEBYSCORE", "z", "-inf", "bad"),
                    invalid("ERR value is not an integer or out of range", "ZREMRANGEBYRANK", "z", "bad", "-1"),
                    invalid("ERR invalid cursor", "ZSCAN", "z", "-1"),
                    invalid("ERR syntax error", "ZSCAN", "z", "0", "MATCH"),
                    invalid("ERR syntax error", "ZSCAN", "z", "0", "COUNT", "0")
            )) {
                assertContentErrorFailsOnlyThatCommand(db, invalid.args(), invalid.message());
            }
        });
    }

    @Test
    public void nullBulkStringInsideMultiAbortsBeforeExec() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject badNull = client.execute(Arrays.asList(b("SET"), b("k"), null));
                Assert.assertTrue(badNull instanceof ReplyError);
                Assert.assertEquals("ERR Protocol error: null bulk string", ((ReplyError) badNull).message());
                Assert.assertEquals(0, session.transactionState().size());

                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
            }
        });
    }

    @Test
    public void pastAbsoluteExpiryInsideMultiQueuesAndExecApplies() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());

                // 墙钟依赖不属于 preflight：过去但为正的 EXAT/PXAT 照常 QUEUED，EXEC 逐条给结果。
                long pastExat = (System.currentTimeMillis() / 1000L) - 60L;
                long pastPxat = System.currentTimeMillis() - 60_000L;
                Assert.assertEquals(
                        "QUEUED",
                        ((ReplySimpleString) client.execute(List.of(
                                b("SET"), b("exat-k"), b("v"), b("EXAT"), b(Long.toString(pastExat))))).value()
                );
                Assert.assertEquals(
                        "QUEUED",
                        ((ReplySimpleString) client.execute(List.of(
                                b("SET"), b("pxat-k"), b("v"), b("PXAT"), b(Long.toString(pastPxat))))).value()
                );

                ReplyObject exec = client.execute(List.of(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyArray);
                ReplyArray results = (ReplyArray) exec;
                Assert.assertNotNull(results.values());
                Assert.assertEquals(2, results.values().size());
                Assert.assertEquals("OK", ((ReplySimpleString) results.values().get(0)).value());
                Assert.assertEquals("OK", ((ReplySimpleString) results.values().get(1)).value());

                // set-then-expire：EXEC 之后两个 key 都不存在。
                Assert.assertSame(ReplyNull.INSTANCE, client.execute(List.of(b("GET"), b("exat-k"))));
                Assert.assertSame(ReplyNull.INSTANCE, client.execute(List.of(b("GET"), b("pxat-k"))));
            }
        });
    }

    @Test
    public void timeIndependentSetErrorsInsideMultiFailOnlyThatCommand() {
        forEachDb(db -> {
            for (InvalidCommand invalid : List.of(
                    invalid("ERR syntax error", "SET", "k", "v", "NOPE"),
                    invalid("ERR invalid expire time in 'set' command", "SET", "k", "v", "EXAT", "0"),
                    invalid("ERR invalid expire time in 'set' command", "SET", "k", "v", "PXAT", "-1")
            )) {
                assertContentErrorFailsOnlyThatCommand(db, invalid.args(), invalid.message());
            }
        });
    }

    @Test
    public void transactionControlParseErrorsAbortAndDiscardQueuedWrites() {
        forEachDb(db -> {
            for (String control : List.of("MULTI", "EXEC", "DISCARD")) {
                CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
                TestSession session = new TestSession();
                byte[] key = b("dirty:" + control.toLowerCase(java.util.Locale.ROOT));
                {
                    FastTestClient client = new FastTestClient(dispatcher, session);
                    Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
                    Assert.assertEquals(
                            "QUEUED",
                            ((ReplySimpleString) client.execute(List.of(b("SET"), key, b("value")))).value()
                    );

                    ReplyObject invalidControl = client.execute(List.of(b(control), b("extra")));
                    Assert.assertTrue(control, invalidControl instanceof ReplyError);
                    Assert.assertEquals(
                            "ERR wrong number of arguments for '" + control.toLowerCase(java.util.Locale.ROOT) + "' command",
                            ((ReplyError) invalidControl).message()
                    );

                    ReplyObject exec = client.execute(List.of(b("EXEC")));
                    Assert.assertTrue(control, exec instanceof ReplyError);
                    Assert.assertEquals(
                            "EXECABORT Transaction discarded because of previous errors.",
                            ((ReplyError) exec).message()
                    );
                    Assert.assertSame(ReplyNull.INSTANCE, client.execute(List.of(b("GET"), key)));
                }
            }
        });
    }

    @Test
    public void testCommandCompositionKeepsTransactionCommandsExplicitlyRegistered() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("DISCARD")))).value());
            }
        });
    }

    @Test
    public void bareAuthOutsideMultiUsesTheSyntaxArity() {
        forEachDb(db -> {
            {
                FastTestClient client = new FastTestClient(TestCommandComposition.createDispatcher(db));
                ReplyError error = (ReplyError) client.execute(List.of(b("AUTH")));
                Assert.assertEquals("ERR wrong number of arguments for 'auth' command", error.message());
            }
        });
    }

    @Test
    public void bareAuthInsideMultiMarksDirtyAndExecDoesNotApplyQueuedWrites() {
        forEachDb(db -> {
            {
                FastTestClient client = new FastTestClient(TestCommandComposition.createDispatcher(db));
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(List.of(b("SET"), b("k"), b("v")))).value());
                ReplyError arity = (ReplyError) client.execute(List.of(b("AUTH")));
                Assert.assertEquals("ERR wrong number of arguments for 'auth' command", arity.message());
                ReplyError abort = (ReplyError) client.execute(List.of(b("EXEC")));
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", abort.message());
                Assert.assertTrue(client.execute(List.of(b("GET"), b("k"))) instanceof ReplyNull);
            }
        });
    }

    private static void assertContentErrorFailsOnlyThatCommand(
            YierdisDb db,
            List<byte[]> invalid,
            String message
    ) {
        CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
        FastTestClient client = new FastTestClient(dispatcher);
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("FLUSHDB")))).value());
        Assert.assertEquals("OK", ((ReplySimpleString) client.execute(List.of(b("MULTI")))).value());
        Assert.assertEquals(
                "QUEUED",
                ((ReplySimpleString) client.execute(List.of(b("SET"), b("kept"), b("1")))).value()
        );
        Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(invalid)).value());

        ReplyArray exec = (ReplyArray) client.execute(List.of(b("EXEC")));
        Assert.assertNotNull(exec.values());
        Assert.assertEquals(2, exec.values().size());
        Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(0)).value());
        ReplyError failure = (ReplyError) exec.values().get(1);
        if (message != null) {
            Assert.assertEquals(message, failure.message());
        }
        Assert.assertEquals("1", ((ReplyBulkString) client.execute(List.of(b("GET"), b("kept")))).asString());
    }

    private static InvalidCommand invalid(String message, String... args) {
        List<byte[]> encoded = new ArrayList<>(args.length);
        for (String arg : args) {
            encoded.add(b(arg));
        }
        return new InvalidCommand(List.copyOf(encoded), message);
    }

    private static final class TestSession implements yier.bubu.redis.execution.api.CommandSession {
        private int dbIndex;
        private String clientName;
        private final TestTransactionState tx = new TestTransactionState();

        @Override
        public int dbIndex() {
            return dbIndex;
        }

        @Override
        public void setDbIndex(int dbIndex) {
            this.dbIndex = Math.max(0, dbIndex);
        }

        @Override
        public String clientName() {
            return clientName;
        }

        @Override
        public void setClientName(String clientName) {
            this.clientName = clientName;
        }

        @Override
        public TransactionState transaction() {
            return tx;
        }

        @Override
        public yier.bubu.redis.execution.api.ConnectionStatsView connectionStats() {
            return null;
        }

        @Override
        public int respVersion() {
            return 2;
        }

        @Override
        public void setRespVersion(int respVersion) {
        }

        private TestTransactionState transactionState() {
            return tx;
        }
    }

    private static final class TestTransactionState implements TransactionState {
        private boolean active;
        private boolean aborted;
        private final ArrayList<ExecutionRequest> queue = new ArrayList<>();

        @Override
        public boolean active() {
            return active;
        }

        @Override
        public void begin() {
            closeQueued();
            active = true;
            aborted = false;
        }

        @Override
        public void discard() {
            closeQueued();
            active = false;
            aborted = false;
        }

        @Override
        public String tryEnqueue(ExecutionRequest request) {
            if (request == null) {
                return null;
            }
            queue.add(ByteArrayExecutionRequest.copyOf(request));
            return null;
        }

        @Override
        public boolean aborted() {
            return aborted;
        }

        @Override
        public void markAborted() {
            aborted = true;
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public void forEachQueued(java.util.function.Consumer<? super ExecutionRequest> visitor) {
            java.util.Objects.requireNonNull(visitor, "visitor");
            queue.forEach(visitor);
        }

        @Override
        public List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            return out;
        }

        private ExecutionRequest queued(int index) {
            return queue.get(index);
        }

        private void closeQueued() {
            for (ExecutionRequest request : queue) {
                request.close();
            }
            queue.clear();
        }
    }
}
