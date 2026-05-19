package yier.bubu.redis.integration.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.integration.command.TestCommandProcessors;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
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

public class YierdisChangeEmissionTest {
    @Test
    public void emitsEventsForRealChangesOnly() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = TestCommandProcessors.forDbWithChangeSink(db, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                events.clear();
                client.execute(cmd("GET", "missing"));
                Assert.assertEquals(0, events.size());

                Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
                Assert.assertEquals(1, events.size());

                YierdisChangeEvent e = events.get(0);
                Assert.assertEquals(0, e.dbIndex());
                Assert.assertNotNull(e.record());
                Assert.assertEquals("SET", arg(e, 0));
                Assert.assertEquals("k", arg(e, 1));
                Assert.assertEquals("v", arg(e, 2));

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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDbWithChangeSink(db, sink);
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
                Assert.assertEquals("SET", arg(events.get(0), 0));
            }
        });
    }

    @Test
    public void zaddScoreUpdateEmitsEventEvenWhenAddedIsZero() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = TestCommandProcessors.forDbWithChangeSink(db, sink);
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

            YierdisFastCommandProcessor processor = TestCommandProcessors.forDbWithChangeSink(db, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(cmd("PFADD", "h1", "a")) instanceof ReplyInteger);

                events.clear();
                ReplyObject merged = client.execute(cmd("PFMERGE", "h", "h1"));
                Assert.assertTrue(merged instanceof ReplySimpleString);
                Assert.assertEquals(1, events.size());
                Assert.assertEquals("PFMERGE", arg(events.get(0), 0));
            }
        });
    }

    @Test
    public void passiveExpirationEmitsSyntheticDeleteForReadPath() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = TestCommandProcessors.forDbWithChangeSink(db, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(cmd("SET", "expiring", "v", "PX", "1")) instanceof ReplySimpleString);
                events.clear();

                sleepPastTtl();

                ReplyObject get = client.execute(cmd("GET", "expiring"));
                Assert.assertTrue(get instanceof ReplyNull);
                assertSyntheticDelete(events, "expiring", YierdisChangeKind.EXPIRED);
            }
        });
    }

    @Test
    public void passiveExpirationEmitsSyntheticDeleteForKeyspaceReadsAndDel() {
        forEachDb(db -> {
            List<YierdisChangeEvent> events = new ArrayList<>();
            YierdisChangeSink sink = events::add;

            YierdisFastCommandProcessor processor = TestCommandProcessors.forDbWithChangeSink(db, sink);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(cmd("SET", "exists-expired", "v", "PX", "1")) instanceof ReplySimpleString);
                events.clear();
                sleepPastTtl();
                ReplyObject exists = client.execute(cmd("EXISTS", "exists-expired"));
                Assert.assertTrue(exists instanceof ReplyInteger);
                Assert.assertEquals(0, ((ReplyInteger) exists).value());
                assertSyntheticDelete(events, "exists-expired", YierdisChangeKind.EXPIRED);

                Assert.assertTrue(client.execute(cmd("SET", "ttl-expired", "v", "PX", "1")) instanceof ReplySimpleString);
                events.clear();
                sleepPastTtl();
                ReplyObject ttl = client.execute(cmd("TTL", "ttl-expired"));
                Assert.assertTrue(ttl instanceof ReplyInteger);
                Assert.assertEquals(-2, ((ReplyInteger) ttl).value());
                assertSyntheticDelete(events, "ttl-expired", YierdisChangeKind.EXPIRED);

                Assert.assertTrue(client.execute(cmd("SET", "del-expired", "v", "PX", "1")) instanceof ReplySimpleString);
                events.clear();
                sleepPastTtl();
                ReplyObject del = client.execute(cmd("DEL", "del-expired"));
                Assert.assertTrue(del instanceof ReplyInteger);
                Assert.assertEquals(0, ((ReplyInteger) del).value());
                assertSyntheticDelete(events, "del-expired", YierdisChangeKind.EXPIRED);
            }
        });
    }

    private static String arg(YierdisChangeEvent event, int index) {
        return new String(event.request().toByteArray(index), StandardCharsets.US_ASCII);
    }

    private static void assertSyntheticDelete(List<YierdisChangeEvent> events, String key, YierdisChangeKind kind) {
        Assert.assertEquals(1, events.size());
        YierdisChangeEvent event = events.get(0);
        Assert.assertTrue(event.synthetic());
        Assert.assertEquals(kind, event.kind());
        Assert.assertEquals("DEL", arg(event, 0));
        Assert.assertEquals(key, arg(event, 1));
    }

    private static void sleepPastTtl() {
        try {
            Thread.sleep(20L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("interrupted while waiting for TTL to pass");
        }
    }

    private static final class TestSession implements ServerSession {
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
    }

    private static final class TestTransactionState implements TransactionState {
        private boolean active;
        private final ArrayList<ExecutionRequest> queue = new ArrayList<>();

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
        public void enqueue(ExecutionRequest request) {
            if (request == null) {
                return;
            }
            queue.add(ByteArrayExecutionRequest.copyOf(request));
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            return out;
        }
    }
}
