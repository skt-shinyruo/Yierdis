package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;
import yier.bubu.redis.testutil.TestPreparedCommands;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class TransactionCommandTest {
    @Test
    public void multiQueuesAndExecAppliesInOrder() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
    public void execAndDiscardWithoutMultiReturnErrors() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(
                    db,
                    registration -> registration.register(new CommandDefinition<>(
                            new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                                    TransactionPolicy.DISALLOWED_IN_MULTI),
                            CommandParsers.args(),
                            (cmd, context) -> TestPreparedCommands.simpleString("HELLO")
                    ))
            );
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(
                    db,
                    registration -> registration.register(new CommandDefinition<>(
                            new CommandSyntax("STRICT", CommandArity.exact(2), CommandKeySpec.NONE,
                                    TransactionPolicy.QUEUEABLE),
                            CommandParsers.args(),
                            (args, context) -> TestPreparedCommands.simpleString("OK")
                    ))
            );
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
    public void setOptionSyntaxInsideMultiAbortsBeforeExecAfterParserMigration() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                ReplyObject badSet = client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("NX"), b("XX")));
                Assert.assertTrue(badSet instanceof ReplyError);
                Assert.assertEquals("ERR syntax error", ((ReplyError) badSet).message());
                Assert.assertEquals(0, session.transactionState().size());

                ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
                Assert.assertTrue(exec instanceof ReplyError);
                Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
            }
        });
    }

    @Test
    public void nullBulkStringInsideMultiAbortsBeforeExec() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
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
    public void transactionControlParseErrorsAbortAndDiscardQueuedWrites() {
        forEachDb(db -> {
            for (String control : List.of("MULTI", "EXEC", "DISCARD")) {
                YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
                TestSession session = new TestSession();
                byte[] key = b("dirty:" + control.toLowerCase(java.util.Locale.ROOT));
                try (FastTestClient client = new FastTestClient(processor, session)) {
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
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("DISCARD")))).value());
            }
        });
    }

    @Test
    public void bareAuthOutsideMultiUsesTheSyntaxArity() {
        forEachDb(db -> {
            try (FastTestClient client = new FastTestClient(TestCommandComposition.createProcessor(db))) {
                ReplyError error = (ReplyError) client.execute(List.of(b("AUTH")));
                Assert.assertEquals("ERR wrong number of arguments for 'auth' command", error.message());
            }
        });
    }

    @Test
    public void bareAuthInsideMultiMarksDirtyAndExecDoesNotApplyQueuedWrites() {
        forEachDb(db -> {
            try (FastTestClient client = new FastTestClient(TestCommandComposition.createProcessor(db))) {
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

    private static final class TestSession implements yier.bubu.redis.execution.api.CommandSession {
        private int dbIndex;
        private String clientName;
        private boolean authenticated;
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
        public long clientId() {
            return 1L;
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
        public boolean authenticated() {
            return authenticated;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
            this.authenticated = authenticated;
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

        @Override
        public void close() {
            discard();
        }

        private void closeQueued() {
            for (ExecutionRequest request : queue) {
                request.close();
            }
            queue.clear();
        }
    }
}
