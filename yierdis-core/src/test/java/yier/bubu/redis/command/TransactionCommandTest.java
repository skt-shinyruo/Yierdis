package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespServerSession;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespTransactionState;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.ArrayList;
import java.util.Arrays;
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
                Assert.assertEquals("OK", ((RespSimpleString) client.execute(Arrays.asList(b("MULTI")))).value());
                Assert.assertEquals("QUEUED", ((RespSimpleString) client.execute(Arrays.asList(b("SET"), b("k"), b("v")))).value());
                Assert.assertEquals("QUEUED", ((RespSimpleString) client.execute(Arrays.asList(b("GET"), b("k")))).value());

                RespArray exec = (RespArray) client.execute(Arrays.asList(b("EXEC")));
                Assert.assertNotNull(exec.values());
                Assert.assertEquals(2, exec.values().size());
                Assert.assertEquals("OK", ((RespSimpleString) exec.values().get(0)).value());
                Assert.assertEquals("v", ((RespBulkString) exec.values().get(1)).asString());

                Assert.assertEquals("v", ((RespBulkString) client.execute(Arrays.asList(b("GET"), b("k")))).asString());
            }
        });
    }

    @Test
    public void execAndDiscardWithoutMultiReturnErrors() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                RespError exec = (RespError) client.execute(Arrays.asList(b("EXEC")));
                Assert.assertEquals("ERR EXEC without MULTI", exec.message());

                RespError discard = (RespError) client.execute(Arrays.asList(b("DISCARD")));
                Assert.assertEquals("ERR DISCARD without MULTI", discard.message());
            }
        });
    }

    @Test
    public void multiCannotBeNested() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((RespSimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

                RespError nested = (RespError) client.execute(Arrays.asList(b("MULTI")));
                Assert.assertEquals("ERR MULTI calls can not be nested", nested.message());

                Assert.assertEquals("OK", ((RespSimpleString) client.execute(Arrays.asList(b("DISCARD")))).value());
            }
        });
    }

    private static final class TestSession implements RespServerSession {
        private RespProtocol protocol = RespProtocol.RESP2;
        private int dbIndex;
        private String clientName;
        private boolean authenticated;
        private final RespTransactionState tx = new TestTransactionState();

        @Override
        public RespProtocol protocol() {
            return protocol;
        }

        @Override
        public void setProtocol(RespProtocol protocol) {
            this.protocol = protocol == null ? RespProtocol.RESP2 : protocol;
        }

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
        public RespTransactionState transaction() {
            return tx;
        }
    }

    private static final class TestTransactionState implements RespTransactionState {
        private boolean active;
        private final ArrayList<byte[][]> queue = new ArrayList<>();

        @Override
        public boolean active() {
            return active;
        }

        @Override
        public void begin() {
            active = true;
            queue.clear();
        }

        @Override
        public void discard() {
            active = false;
            queue.clear();
        }

        @Override
        public void enqueue(byte[][] argv) {
            queue.add(argv);
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
            return out;
        }
    }
}
