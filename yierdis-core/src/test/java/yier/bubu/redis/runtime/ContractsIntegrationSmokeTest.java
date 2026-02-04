package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespServerSession;
import yier.bubu.redis.protocol.RespTransactionState;
import yier.bubu.redis.testutil.FastTestClient;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

public class ContractsIntegrationSmokeTest {
    @Test
    public void smokeAcrossBackendsAndMaxmemoryScopes() {
        runCase(false, YierdisInstanceConfig.MaxmemoryScope.GLOBAL);
        runCase(false, YierdisInstanceConfig.MaxmemoryScope.PER_DB);
        runCase(true, YierdisInstanceConfig.MaxmemoryScope.GLOBAL);
        runCase(true, YierdisInstanceConfig.MaxmemoryScope.PER_DB);
    }

    private static void runCase(boolean offHeapValues, YierdisInstanceConfig.MaxmemoryScope scope) {
        YierdisUnsafeOffHeapAllocator allocator = offHeapValues ? new YierdisUnsafeOffHeapAllocator(5_000) : null;
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .offHeapAllocator(allocator)
                .ownsOffHeapAllocator(allocator != null)
                .maxmemoryScope(scope)
                .maxmemoryBytes(2_000)
                .maxmemoryPolicy("noeviction")
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = instance.newCommandProcessor();
            TestSession session = new TestSession();

            try (FastTestClient client = new FastTestClient(processor, session)) {
                // TTL family: basic Redis-like conventions.
                Assert.assertEquals(-2L, ((RespInteger) client.execute(Arrays.asList(b("TTL"), b("missing")))).value());

                client.execute(Arrays.asList(b("SET"), b("k"), b("v")));
                Assert.assertEquals(-1L, ((RespInteger) client.execute(Arrays.asList(b("PTTL"), b("k")))).value());
                Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("EXPIRE"), b("k"), b("60")))).value());
                Assert.assertTrue(((RespInteger) client.execute(Arrays.asList(b("TTL"), b("k")))).value() > 0L);

                // SCAN must always be able to make progress and terminate at cursor=0 (best-effort).
                for (int i = 0; i < 20; i++) {
                    client.execute(Arrays.asList(b("SET"), b("scan:" + i), b("v")));
                }
                long cursor = 0L;
                for (int round = 0; round < 50; round++) {
                    RespArray reply = (RespArray) client.execute(Arrays.asList(
                            b("SCAN"),
                            Long.toString(cursor).getBytes(StandardCharsets.US_ASCII),
                            b("MATCH"), b("scan:*"),
                            b("COUNT"), b("3")
                    ));
                    cursor = parseCursor(reply);
                    if (cursor == 0L) {
                        break;
                    }
                }
                Assert.assertEquals(0L, cursor);

                // maxmemory/off-heap OOM: rejection happens before reply is written (no double reply) and is stable.
                byte[] big = new byte[900];
                Arrays.fill(big, (byte) 'a');
                boolean sawOom = false;
                for (int i = 0; i < 50; i++) {
                    // Alternate DB index to exercise global/per-db coordination.
                    client.execute(Arrays.asList(b("SELECT"), b(Integer.toString(i & 1))));
                    Object reply = client.execute(Arrays.asList(b("SET"), b("oom:" + i), big));
                    if (reply instanceof RespError) {
                        sawOom = true;
                        break;
                    }
                }
                Assert.assertTrue("expected to eventually see OOM under bounded budgets", sawOom);
            }
        }
    }

    private static long parseCursor(RespArray reply) {
        Assert.assertNotNull(reply);
        Assert.assertNotNull(reply.values());
        Assert.assertEquals(2, reply.values().size());
        RespBulkString cursorOut = (RespBulkString) reply.values().get(0);
        Assert.assertNotNull(cursorOut.data());
        return Long.parseLong(new String(cursorOut.data(), StandardCharsets.US_ASCII));
    }

    private static final class TestSession implements RespServerSession {
        private RespProtocol protocol = RespProtocol.RESP2;
        private int dbIndex;
        private final RespTransactionState tx = new NoopTransactionState();

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
            return null;
        }

        @Override
        public void setClientName(String clientName) {
        }

        @Override
        public boolean authenticated() {
            return false;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
        }

        @Override
        public RespTransactionState transaction() {
            return tx;
        }
    }

    private static final class NoopTransactionState implements RespTransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void discard() {
        }

        @Override
        public void enqueue(byte[][] argv) {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public java.util.List<byte[][]> drain() {
            return java.util.Collections.emptyList();
        }
    }
}
