package yier.bubu.redis.runtime.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class YierdisChangeSinkTest {
    @Test
    public void emitsEventsForRealChangesOnly() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db, null, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                events.clear();
                client.execute(cmd("GET", "missing"));
                Assert.assertEquals(0, events.size());

                Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
                Assert.assertEquals(1, events.size());

                YierdisChangeEvent e = events.get(0);
                Assert.assertEquals(0, e.dbIndex());
                Assert.assertEquals("SET", new String(e.argv()[0], StandardCharsets.US_ASCII));
                Assert.assertEquals("k", new String(e.argv()[1], StandardCharsets.US_ASCII));
                Assert.assertEquals("v", new String(e.argv()[2], StandardCharsets.US_ASCII));

                // No-op writes: should not emit.
                events.clear();
                ReplyObject nx = client.execute(cmd("SET", "k", "v2", "NX"));
                Assert.assertTrue(nx instanceof ReplyNull);
                Assert.assertEquals(0, events.size());

                events.clear();
                ReplyObject delMissing = client.execute(cmd("DEL", "missing"));
                Assert.assertTrue(delMissing instanceof ReplyInteger);
                Assert.assertEquals(0, ((ReplyInteger) delMissing).value());
                Assert.assertEquals(0, events.size());

                events.clear();
                ReplyObject expireMissing = client.execute(cmd("EXPIRE", "missing", "10"));
                Assert.assertTrue(expireMissing instanceof ReplyInteger);
                Assert.assertEquals(0, ((ReplyInteger) expireMissing).value());
                Assert.assertEquals(0, events.size());

                events.clear();
                ReplyObject persistNoTtl = client.execute(cmd("PERSIST", "k"));
                Assert.assertTrue(persistNoTtl instanceof ReplyInteger);
                Assert.assertEquals(0, ((ReplyInteger) persistNoTtl).value());
                Assert.assertEquals(0, events.size());

                // TTL metadata changes: should emit.
                events.clear();
                ReplyObject expireOk = client.execute(cmd("EXPIRE", "k", "10"));
                Assert.assertTrue(expireOk instanceof ReplyInteger);
                Assert.assertEquals(1, ((ReplyInteger) expireOk).value());
                Assert.assertEquals(1, events.size());

                events.clear();
                ReplyObject persistOk = client.execute(cmd("PERSIST", "k"));
                Assert.assertTrue(persistOk instanceof ReplyInteger);
                Assert.assertEquals(1, ((ReplyInteger) persistOk).value());
                Assert.assertEquals(1, events.size());
            }
        });
    }

    @Test
    public void transactionExecEmitsOnlyForRealWrites() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            TestSession session = new TestSession();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db, null, sink);
            try (FastTestClient client = new FastTestClient(processor, session)) {
                events.clear();
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd("MULTI"))).value());
                Assert.assertEquals(0, events.size());

                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(cmd("SET", "k", "v"))).value());
                Assert.assertEquals(0, events.size());

                Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(cmd("GET", "k"))).value());
                Assert.assertEquals(0, events.size());

                ReplyArray exec = (ReplyArray) client.execute(cmd("EXEC"));
                Assert.assertNotNull(exec.values());
                Assert.assertEquals(2, exec.values().size());

                Assert.assertEquals(1, events.size());
                Assert.assertEquals("SET", new String(events.get(0).argv()[0], StandardCharsets.US_ASCII));
            }
        });
    }

    @Test
    public void zaddScoreUpdateEmitsEventEvenWhenAddedIsZero() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db, null, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                events.clear();
                ReplyObject first = client.execute(cmd("ZADD", "z", "1", "m"));
                Assert.assertTrue(first instanceof ReplyInteger);
                Assert.assertEquals(1, ((ReplyInteger) first).value());
                Assert.assertEquals(1, events.size());

                events.clear();
                ReplyObject updateScore = client.execute(cmd("ZADD", "z", "2", "m"));
                Assert.assertTrue(updateScore instanceof ReplyInteger);
                Assert.assertEquals(0, ((ReplyInteger) updateScore).value());
                Assert.assertEquals(1, events.size());
            }
        });
    }

    @Test
    public void pfmergeEmitsEvent() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db, null, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(cmd("PFADD", "h1", "a")) instanceof ReplyInteger);

                events.clear();
                ReplyObject merged = client.execute(cmd("PFMERGE", "h", "h1"));
                Assert.assertTrue(merged instanceof ReplySimpleString);
                Assert.assertEquals(1, events.size());
                Assert.assertEquals("PFMERGE", new String(events.get(0).argv()[0], StandardCharsets.US_ASCII));
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
