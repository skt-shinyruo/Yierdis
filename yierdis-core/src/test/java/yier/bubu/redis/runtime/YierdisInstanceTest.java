package yier.bubu.redis.runtime;

// YierdisInstanceTest：覆盖可嵌入 instance 的装配语义与关键不变量（多 DB + global maxmemory + shared off-heap 等）。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespServerSession;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespTransactionState;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static yier.bubu.redis.testutil.TestBytes.b;

public class YierdisInstanceTest {
    @Test
    public void globalMaxmemoryCountsSharedOffheapOnceAcrossDbs() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .offHeapAllocator(allocator)
                .ownsOffHeapAllocator(true)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(9000)
                .maxmemoryPolicy("noeviction")
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = instance.newCommandProcessor();
            TestSession session = new TestSession();
            byte[] value = new byte[4000];
            Arrays.fill(value, (byte) 'a');

            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((RespSimpleString) client.execute(Arrays.asList(b("SET"), b("k0"), value))).value());
                Assert.assertEquals("OK", ((RespSimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());

                // 若 shared allocator 被按 DB 重复计入 maxmemory，这里通常会提前 OOM。
                Object reply = client.execute(Arrays.asList(b("SET"), b("k1"), value));
                Assert.assertFalse("expected not OOM (no double-count off-heap)", reply instanceof RespError);
                Assert.assertEquals("OK", ((RespSimpleString) reply).value());
            }

            Assert.assertTrue("expected off-heap allocations", allocator.usedBytes() > 0);
        }
    }

    @Test
    public void unboundOrCrossThreadAccessFailsFast() throws Exception {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder().build();
        try (YierdisInstance instance = YierdisInstance.create(config)) {
            try {
                instance.db(0).size();
                Assert.fail("expected fail-fast before bind");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("bindToCurrentThread"));
            }

            instance.bindToCurrentThread();

            AtomicReference<Throwable> errRef = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    instance.db(0).size();
                } catch (Throwable t1) {
                    errRef.set(t1);
                }
            });
            t.start();
            t.join();

            Throwable err = errRef.get();
            Assert.assertNotNull("expected error from non-owner thread", err);
            Assert.assertTrue("expected IllegalStateException", err instanceof IllegalStateException);
            Assert.assertTrue(err.getMessage().contains("non-owner thread"));
        }
    }

    private static final class TestSession implements RespServerSession {
        private RespProtocol protocol = RespProtocol.RESP2;
        private int dbIndex;
        private String clientName;
        private boolean authenticated;
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

