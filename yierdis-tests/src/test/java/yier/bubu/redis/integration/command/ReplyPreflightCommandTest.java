package yier.bubu.redis.integration.command;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyRenderer;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.KeyspaceReadOps;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.storage.api.result.PayloadLengthSink;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ReplyPreflightCommandTest {
    private static final RespReplySizer REPLY_SIZER = new RespReplySizer();

    @Test
    public void setGetCapacityRejectionLeavesTheExistingValueUntouched() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("SET", "key", "old"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                ReplyPlan expectedOldValuePlan = execute(
                        dispatcher,
                        session,
                        new TrackingReplyWriter(),
                        cmd("GET", "key")
                );
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(dispatcher, session, capacity, writer, cmd("SET", "key", "new", "GET"))
                );

                Assert.assertArrayEquals(b("old"), db.reads().strings().getStringBytes(b("key")));
                Assert.assertEquals(expectedOldValuePlan, capacity.reservedPlan());
            }
        });
    }

    @Test
    public void setNxGetCapacityRejectionReservesTheExistingValueReply() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("SET", "key", "old"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                ReplyPlan expectedOldValuePlan = execute(
                        dispatcher,
                        session,
                        new TrackingReplyWriter(),
                        cmd("GET", "key")
                );
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(dispatcher, session, capacity, writer, cmd("SET", "key", "new", "NX", "GET"))
                );

                Assert.assertArrayEquals(b("old"), db.reads().strings().getStringBytes(b("key")));
                Assert.assertEquals(expectedOldValuePlan, capacity.reservedPlan());
            }
        });
    }

    @Test
    public void countedPopCapacityRejectionLeavesTheListUntouched() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("RPUSH", "list", "a", "bb"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(dispatcher, session, capacity, writer, cmd("LPOP", "list", "2"))
                );

                try (ByteSequenceSource values = db.reads().lists().lrange(b("list"), 0, -1)) {
                    Assert.assertEquals(2, values.elementCount());
                }
                Assert.assertEquals(19L, capacity.reservedPlan().encodedUpperBoundBytes());
            }
        });
    }

    @Test
    public void uncountedPopCapacityRejectionLeavesTheListUntouched() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("RPUSH", "list", "value"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(dispatcher, session, capacity, writer, cmd("LPOP", "list"))
                );

                try (ByteSequenceSource values = db.reads().lists().lrange(b("list"), 0, -1)) {
                    Assert.assertEquals(1, values.elementCount());
                }
                Assert.assertEquals(11L, capacity.reservedPlan().encodedUpperBoundBytes());
                Assert.assertTrue(capacity.reservedPlan().retainedSourceBytes() > 0L);
            }
        });
    }

    @Test
    public void getHgetAndEchoPrepareTheirSemanticReplySources() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("SET", "string", "value"));
                client.execute(cmd("HSET", "hash", "field", "value"));

                assertScalarReplySource(dispatcher, cmd("GET", "string"), 5);
                assertScalarReplySource(dispatcher, cmd("HGET", "hash", "field"), 5);

                ExecutionRequest echo = ByteArrayExecutionRequest.fromUtf8("ECHO", List.of("message"));
                TrackingReplyWriter echoWriter = new TrackingReplyWriter();
                EngineSession session = new EngineSession(16, 16 * 1024L);
                long retainedBytes = echo.admittedMemoryBytes();
                ReplyPlan plan = execute(dispatcher, session, echoWriter, echo);
                Assert.assertEquals(
                        REPLY_SIZER.plan(session, ReplyShapes.bulkString(7, retainedBytes)),
                        plan
                );
            }
        });
    }

    @Test
    public void aggregateRepliesReserveTheirExactWireShapeBeforeWriting() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("RPUSH", "list", "a", "bb"));
                client.execute(cmd("HSET", "hash", "field", "value"));
                client.execute(cmd("SADD", "set", "a", "bb"));
                client.execute(cmd("ZADD", "zset", "1", "a"));

                assertAggregatePreflight(dispatcher, cmd("LRANGE", "list", "0", "-1"), 19L);
                assertAggregatePreflight(dispatcher, cmd("HGETALL", "hash"), 26L);
                assertAggregatePreflight(dispatcher, cmd("SMEMBERS", "set"), 19L);
                assertAggregatePreflight(dispatcher, cmd("ZRANGE", "zset", "0", "-1"), 11L);
            }
        });
    }

    @Test
    public void memoryStatsPreflightMatchesItsResp2WireShape() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            TrackingReplyWriter writer = new TrackingReplyWriter();
            EngineSession session = new EngineSession(16, 16 * 1024L);

            ReplyPlan plan = execute(dispatcher, session, writer, cmd("MEMORY", "STATS"));

            Assert.assertEquals(writer.emittedResp2Bytes(), plan.encodedUpperBoundBytes());
            Assert.assertEquals(0L, plan.retainedSourceBytes());
        });
    }

    @Test
    public void keyDiscoveryRepliesReserveBeforeWritingTheirNestedWireShape() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("SET", "key", "value"));

                assertAggregatePreflight(dispatcher, cmd("KEYS", "*"), 13L);
                assertAggregatePreflight(dispatcher, cmd("SCAN", "0"), 24L);
            }
        });
    }

    @Test
    public void staleKeyScanWindowClosesBeforeTheSameRequestIsPreparedAgain() {
        forEachDb(db -> {
            AtomicInteger firstWindowCloses = new AtomicInteger();
            AtomicInteger scanCalls = new AtomicInteger();
            KeyspaceReadOps keyspace = staleFirstScan(db.reads().keyspace(), scanCalls, firstWindowCloses);
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(withKeyspaceReads(db, keyspace));

            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyArray reply = (ReplyArray) client.execute(cmd("SCAN", "0"));
                Assert.assertNotNull(reply.values());
            }

            Assert.assertEquals(2, scanCalls.get());
            Assert.assertEquals(1, firstWindowCloses.get());
        });
    }

    @Test
    public void collectionScanReservesItsNestedShapeAndMaterializedWindow() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("HSET", "hash", "field", "value"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(dispatcher, session, capacity, writer, cmd("HSCAN", "hash", "0"))
                );
                Assert.assertEquals(37L, capacity.reservedPlan().encodedUpperBoundBytes());
                Assert.assertTrue(capacity.reservedPlan().retainedSourceBytes() > 0L);
            }
        });
    }

    @Test
    public void nativeCollectionScanChargesPinnedPayloadAndRejectsBeforeEmission() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                String largeMember = "x".repeat(256 * 1024);
                client.execute(cmd("SADD", "large-set", largeMember));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(
                                dispatcher,
                                session,
                                capacity,
                                writer,
                                cmd("SSCAN", "large-set", "0", "COUNT", Integer.toString(Integer.MAX_VALUE))
                        )
                );

                Assert.assertTrue(capacity.reservedPlan().encodedUpperBoundBytes() > largeMember.length());
                Assert.assertTrue(capacity.reservedPlan().retainedSourceBytes() >= largeMember.length());
                Assert.assertEquals(0, writer.arrayHeaderCalls());
            }
        });
    }

    @Test
    public void execCapacityRejectionDoesNotDrainOrExecuteTheQueuedTransaction() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            EngineSession session = new EngineSession(16, 16 * 1024L);
            TrackingReplyWriter accepting = new TrackingReplyWriter();

            execute(dispatcher, session, accepting, cmd("MULTI"));
            execute(dispatcher, session, accepting, cmd("SET", "queued", "value"));
            Assert.assertTrue(session.transaction().active());
            Assert.assertEquals(1, session.transaction().size());

            TrackingReplyWriter rejecting = new TrackingReplyWriter();
            CapacityGate capacity = CapacityGate.rejecting();
            Assert.assertThrows(
                    ReplyCapacityUnavailableException.class,
                    () -> execute(dispatcher, session, capacity, rejecting, cmd("EXEC"))
            );

            Assert.assertTrue(session.transaction().active());
            Assert.assertEquals(1, session.transaction().size());
            Assert.assertNull(db.reads().strings().getStringBytes(b("queued")));
            Assert.assertEquals(ReplyPlan.maximum(), capacity.reservedPlan());
            session.discardTransaction();
        });
    }

    @Test
    public void execWithStateDependentQueuedReadReservesMaximumBeforeMutating() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            EngineSession session = new EngineSession(16, 16 * 1024L);
            TrackingReplyWriter accepting = new TrackingReplyWriter();

            execute(dispatcher, session, accepting, cmd("MULTI"));
            execute(dispatcher, session, accepting, cmd("SET", "queued", "value"));
            execute(dispatcher, session, accepting, cmd("GET", "queued"));
            Assert.assertEquals(2, session.transaction().size());

            TrackingReplyWriter rejecting = new TrackingReplyWriter();
            CapacityGate capacity = CapacityGate.rejecting();
            Assert.assertThrows(
                    ReplyCapacityUnavailableException.class,
                    () -> execute(dispatcher, session, capacity, rejecting, cmd("EXEC"))
            );

            Assert.assertEquals(ReplyPlan.maximum(), capacity.reservedPlan());
            Assert.assertEquals(2, session.transaction().size());
            Assert.assertNull(db.reads().strings().getStringBytes(b("queued")));
            Assert.assertEquals(0, rejecting.arrayHeaderCalls());
            session.discardTransaction();
        });
    }

    private static void assertScalarReplySource(
            CommandDispatcher dispatcher,
            List<byte[]> command,
            int payloadLength
    ) {
        EngineSession session = new EngineSession(16, 16 * 1024L);
        ReplyPlan plan = execute(dispatcher, session, new TrackingReplyWriter(), command);
        Assert.assertEquals(
                REPLY_SIZER.plan(session, ReplyShapes.bulkString(payloadLength, plan.retainedSourceBytes())),
                plan
        );
    }

    private static DbEngine withKeyspaceReads(DbEngine delegate, KeyspaceReadOps keyspace) {
        return new DbEngine() {
            @Override
            public DbReads reads() {
                DbReads reads = delegate.reads();
                return new DbReads() {
                    @Override
                    public yier.bubu.redis.storage.api.StringReadOps strings() {
                        return reads.strings();
                    }

                    @Override
                    public yier.bubu.redis.storage.api.HashReadOps hashes() {
                        return reads.hashes();
                    }

                    @Override
                    public yier.bubu.redis.storage.api.ListReadOps lists() {
                        return reads.lists();
                    }

                    @Override
                    public yier.bubu.redis.storage.api.SetReadOps sets() {
                        return reads.sets();
                    }

                    @Override
                    public yier.bubu.redis.storage.api.ZSetReadOps zsets() {
                        return reads.zsets();
                    }

                    @Override
                    public yier.bubu.redis.storage.api.HllReadOps hll() {
                        return reads.hll();
                    }

                    @Override
                    public KeyspaceReadOps keyspace() {
                        return keyspace;
                    }

                    @Override
                    public yier.bubu.redis.storage.api.TtlReadOps ttl() {
                        return reads.ttl();
                    }
                };
            }

            @Override
            public yier.bubu.redis.storage.api.DbWrites writes() {
                return delegate.writes();
            }

            @Override
            public yier.bubu.redis.storage.api.MemoryOps memory() {
                return delegate.memory();
            }

            @Override
            public yier.bubu.redis.storage.api.DbLifecycleOps lifecycle() {
                return delegate.lifecycle();
            }

            @Override
            public yier.bubu.redis.storage.api.DbHealthSnapshot health() {
                return delegate.health();
            }
        };
    }

    private static KeyspaceReadOps staleFirstScan(
            KeyspaceReadOps delegate,
            AtomicInteger scanCalls,
            AtomicInteger firstWindowCloses
    ) {
        return new KeyspaceReadOps() {
            @Override
            public ValueType typeOf(BytesView keyView) {
                return delegate.typeOf(keyView);
            }

            @Override
            public boolean existsKey(BytesView keyView) {
                return delegate.existsKey(keyView);
            }

            @Override
            public KeyScanWindow keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
                return delegate.keys(globPattern, maxMatches, timeBudgetNanos);
            }

            @Override
            public KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, int count) {
                int call = scanCalls.incrementAndGet();
                if (call == 2) {
                    Assert.assertEquals("stale window must close before reprepare", 1, firstWindowCloses.get());
                }
                KeyScanWindow window = delegate.scan(cursor, globPattern, count);
                return call == 1 ? staleAfterPreparation(window, firstWindowCloses) : window;
            }
        };
    }

    private static KeyScanWindow staleAfterPreparation(KeyScanWindow delegate, AtomicInteger closes) {
        return new KeyScanWindow() {
            private int currentChecks;
            private boolean closed;

            @Override
            public ScanCursorV2 nextCursor() {
                return delegate.nextCursor();
            }

            @Override
            public long inspectedSlots() {
                return delegate.inspectedSlots();
            }

            @Override
            public long tableGeneration() {
                return delegate.tableGeneration();
            }

            @Override
            public long expiryEvaluationMillis() {
                return delegate.expiryEvaluationMillis();
            }

            @Override
            public boolean current() {
                return ++currentChecks == 1 && delegate.current();
            }

            @Override
            public int elementCount() {
                return delegate.elementCount();
            }

            @Override
            public long retainedMemoryBytes() {
                return delegate.retainedMemoryBytes();
            }

            @Override
            public void visitElementLengths(PayloadLengthSink out) {
                delegate.visitElementLengths(out);
            }

            @Override
            public void emitTo(ByteValueSink out) {
                delegate.emitTo(out);
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;
                closes.incrementAndGet();
                delegate.close();
            }
        };
    }

    private static void assertAggregatePreflight(
            CommandDispatcher dispatcher,
            List<byte[]> command,
            long expectedEncodedBytes
    ) {
        EngineSession session = new EngineSession(16, 16 * 1024L);
        CapacityGate capacity = CapacityGate.rejecting();
        Assert.assertThrows(
                ReplyCapacityUnavailableException.class,
                () -> execute(dispatcher, session, capacity, new TrackingReplyWriter(), command)
        );
        Assert.assertEquals(expectedEncodedBytes, capacity.reservedPlan().encodedUpperBoundBytes());
        Assert.assertEquals(0L, capacity.reservedPlan().retainedSourceBytes());
    }

    private static ReplyPlan execute(
            CommandDispatcher dispatcher,
            EngineSession session,
            RedisReplyWriter writer,
            List<byte[]> command
    ) {
        return execute(dispatcher, session, CapacityGate.accepting(), writer, ByteArrayExecutionRequest.copyOf(command));
    }

    private static ReplyPlan execute(
            CommandDispatcher dispatcher,
            EngineSession session,
            RedisReplyWriter writer,
            ExecutionRequest request
    ) {
        return execute(dispatcher, session, CapacityGate.accepting(), writer, request);
    }

    private static ReplyPlan execute(
            CommandDispatcher dispatcher,
            EngineSession session,
            CapacityGate capacity,
            RedisReplyWriter writer,
            List<byte[]> command
    ) {
        return execute(dispatcher, session, capacity, writer, ByteArrayExecutionRequest.copyOf(command));
    }

    private static ReplyPlan execute(
            CommandDispatcher dispatcher,
            EngineSession session,
            CapacityGate capacity,
            RedisReplyWriter writer,
            ExecutionRequest request
    ) {
        try {
            try (PreparedCommand prepared = dispatcher.prepare(session, request)) {
                Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
                ReplyPlan plan = REPLY_SIZER.plan(session, prepared.reservationShape());
                capacity.reserve(plan);
                CommandResult result;
                try (CommandExecutionContext execution = CommandExecutionContext.forRequest(session, request)) {
                    result = prepared.execute(execution);
                }
                RedisReplyRenderer.render(result.reply(), writer);
                if (result.closeAfterReply()) {
                    writer.requestCloseAfterReply();
                }
                return plan;
            }
        } finally {
            request.close();
        }
    }

    private static final class CapacityGate {
        private final boolean reject;
        private ReplyPlan reservedPlan;

        private CapacityGate(boolean reject) {
            this.reject = reject;
        }

        static CapacityGate accepting() {
            return new CapacityGate(false);
        }

        static CapacityGate rejecting() {
            return new CapacityGate(true);
        }

        void reserve(ReplyPlan plan) {
            Assert.assertNull("expected one reply reservation", reservedPlan);
            reservedPlan = plan;
            if (reject) {
                throw new ReplyCapacityUnavailableException("injected reply capacity rejection");
            }
        }

        ReplyPlan reservedPlan() {
            Assert.assertNotNull("expected reply reservation", reservedPlan);
            return reservedPlan;
        }
    }

    private static final class TrackingReplyWriter implements RedisReplyWriter {
        private boolean closeAfterReply;
        private int arrayHeaderCalls;
        private long emittedResp2Bytes;

        int arrayHeaderCalls() {
            return arrayHeaderCalls;
        }

        long emittedResp2Bytes() {
            return emittedResp2Bytes;
        }

        @Override
        public void requestCloseAfterReply() {
            closeAfterReply = true;
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return closeAfterReply;
        }

        @Override
        public void simpleString(String value) {
        }

        @Override
        public void error(String message) {
        }

        @Override
        public void integer(long value) {
            emittedResp2Bytes += 3L + Long.toString(value).length();
        }

        @Override
        public void booleanValue(boolean value) {
        }

        @Override
        public void doubleValue(double value) {
        }

        @Override
        public void bigNumberAscii(String value) {
        }

        @Override
        public void verbatimString(String format, byte[] data) {
        }

        @Override
        public void blobError(String message) {
        }

        @Override
        public void nullValue() {
        }

        @Override
        public void nullArray() {
        }

        @Override
        public void arrayHeader(int count) {
            arrayHeaderCalls++;
            emittedResp2Bytes += 3L + Integer.toString(count).length();
        }

        @Override
        public void emptyArray() {
        }

        @Override
        public void mapHeader(int pairs) {
            int elements = pairs * 2;
            emittedResp2Bytes += 3L + Integer.toString(elements).length();
        }

        @Override
        public void setHeader(int count) {
        }

        @Override
        public void pushHeader(int count) {
        }

        @Override
        public void attributeHeader(int pairs) {
        }

        @Override
        public void bulkString(byte[] data) {
            if (data == null) {
                emittedResp2Bytes += 5L;
                return;
            }
            emittedResp2Bytes += bulkStringEncodedBytes(data.length);
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            if (data == null) {
                bulkString((byte[]) null);
                return;
            }
            emittedResp2Bytes += bulkStringEncodedBytes(len);
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                bulkString((byte[]) null);
                return;
            }
            emittedResp2Bytes += bulkStringEncodedBytes(slice.length());
        }

        @Override
        public void bulkStringLongAscii(long value) {
            emittedResp2Bytes += bulkStringEncodedBytes(Long.toString(value).length());
        }

        private static long bulkStringEncodedBytes(int payloadBytes) {
            return 5L + Integer.toString(payloadBytes).length() + payloadBytes;
        }
    }
}
