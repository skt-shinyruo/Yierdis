package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class TransactionCommandTest {
    @Test
    public void multiQueuesAndExecAppliesInOrder() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                byte[] key = b("k");
                byte[] value = b("v1");
                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), key, value))).value());

                // Mutate the original value buffer after it was enqueued.
                value[1] = (byte) '2';

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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                    db,
                    null,
                    SlowCommandGovernor.DEFAULT,
                    registration -> registration.registerDisallowedInMulti(
                            "HELLO",
                            (cmd, ctx) -> ctx.out().simpleString("HELLO"),
                            CommandDescriptor.of(-1, 0, 0, 0),
                            "ERR HELLO is not allowed in MULTI"
                    )
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

    private static final class TestSession implements ServerSession {
        private int dbIndex;
        private String clientName;
        private boolean authenticated;
        private final TransactionState tx = new TestTransactionState();

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
    }

    private static final class TestTransactionState implements TransactionState {
        private boolean active;
        private boolean aborted;
        private final ArrayList<byte[][]> queue = new ArrayList<>();

        @Override
        public boolean active() {
            return active;
        }

        @Override
        public void begin() {
            active = true;
            aborted = false;
            queue.clear();
        }

        @Override
        public void discard() {
            active = false;
            aborted = false;
            queue.clear();
        }

        @Override
        public void enqueue(byte[][] argv) {
            queue.add(argv);
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
        public List<byte[][]> drain() {
            ArrayList<byte[][]> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            return out;
        }
    }
}
