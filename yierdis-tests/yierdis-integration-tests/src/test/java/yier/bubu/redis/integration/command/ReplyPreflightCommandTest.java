package yier.bubu.redis.integration.command;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.testutil.FastTestClient;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ReplyPreflightCommandTest {
    private static final RespReplySizer REPLY_SIZER = new RespReplySizer();

    @Test
    public void setGetCapacityRejectionLeavesTheExistingValueUntouched() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "key", "old"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                ReplyPlan expectedOldValuePlan = execute(
                        processor,
                        session,
                        new TrackingReplyWriter(),
                        cmd("GET", "key")
                );
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, session, capacity, writer, cmd("SET", "key", "new", "GET"))
                );

                Assert.assertArrayEquals(b("old"), db.reads().strings().getStringBytes(b("key")));
                Assert.assertEquals(expectedOldValuePlan, capacity.reservedPlan());
            }
        });
    }

    @Test
    public void setNxGetCapacityRejectionReservesTheExistingValueReply() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "key", "old"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                ReplyPlan expectedOldValuePlan = execute(
                        processor,
                        session,
                        new TrackingReplyWriter(),
                        cmd("GET", "key")
                );
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, session, capacity, writer, cmd("SET", "key", "new", "NX", "GET"))
                );

                Assert.assertArrayEquals(b("old"), db.reads().strings().getStringBytes(b("key")));
                Assert.assertEquals(expectedOldValuePlan, capacity.reservedPlan());
            }
        });
    }

    @Test
    public void countedPopCapacityRejectionLeavesTheListUntouched() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("RPUSH", "list", "a", "bb"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, session, capacity, writer, cmd("LPOP", "list", "2"))
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("RPUSH", "list", "value"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, session, capacity, writer, cmd("LPOP", "list"))
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "string", "value"));
                client.execute(cmd("HSET", "hash", "field", "value"));

                assertScalarReplySource(processor, cmd("GET", "string"), 5);
                assertScalarReplySource(processor, cmd("HGET", "hash", "field"), 5);

                ExecutionRequest echo = ByteArrayExecutionRequest.fromUtf8("ECHO", List.of("message"));
                TrackingReplyWriter echoWriter = new TrackingReplyWriter();
                EngineSession session = new EngineSession(16, 16 * 1024L);
                long retainedBytes = echo.admittedMemoryBytes();
                ReplyPlan plan = execute(processor, session, echoWriter, echo);
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("RPUSH", "list", "a", "bb"));
                client.execute(cmd("HSET", "hash", "field", "value"));
                client.execute(cmd("SADD", "set", "a", "bb"));
                client.execute(cmd("ZADD", "zset", "1", "a"));

                assertAggregatePreflight(processor, cmd("LRANGE", "list", "0", "-1"), 19L);
                assertAggregatePreflight(processor, cmd("HGETALL", "hash"), 26L);
                assertAggregatePreflight(processor, cmd("SMEMBERS", "set"), 19L);
                assertAggregatePreflight(processor, cmd("ZRANGE", "zset", "0", "-1"), 11L);
            }
        });
    }

    @Test
    public void memoryStatsPreflightMatchesItsResp2WireShape() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            TrackingReplyWriter writer = new TrackingReplyWriter();
            EngineSession session = new EngineSession(16, 16 * 1024L);

            ReplyPlan plan = execute(processor, session, writer, cmd("MEMORY", "STATS"));

            Assert.assertEquals(writer.emittedResp2Bytes(), plan.encodedUpperBoundBytes());
            Assert.assertEquals(0L, plan.retainedSourceBytes());
        });
    }

    @Test
    public void keyDiscoveryRepliesReserveBeforeWritingTheirNestedWireShape() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "key", "value"));

                assertAggregatePreflight(processor, cmd("KEYS", "*"), 13L);
                assertAggregatePreflight(processor, cmd("SCAN", "0"), 24L);
            }
        });
    }

    @Test
    public void collectionScanReservesItsNestedShapeAndMaterializedWindow() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("HSET", "hash", "field", "value"));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, session, capacity, writer, cmd("HSCAN", "hash", "0"))
                );
                Assert.assertEquals(37L, capacity.reservedPlan().encodedUpperBoundBytes());
                Assert.assertTrue(capacity.reservedPlan().retainedSourceBytes() > 0L);
            }
        });
    }

    @Test
    public void nativeCollectionScanChargesPinnedPayloadAndRejectsBeforeEmission() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                String largeMember = "x".repeat(256 * 1024);
                client.execute(cmd("SADD", "large-set", largeMember));

                EngineSession session = new EngineSession(16, 16 * 1024L);
                TrackingReplyWriter writer = new TrackingReplyWriter();
                CapacityGate capacity = CapacityGate.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(
                                processor,
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            EngineSession session = new EngineSession(16, 16 * 1024L);
            TrackingReplyWriter accepting = new TrackingReplyWriter();

            execute(processor, session, accepting, cmd("MULTI"));
            execute(processor, session, accepting, cmd("SET", "queued", "value"));
            Assert.assertTrue(session.transaction().active());
            Assert.assertEquals(1, session.transaction().size());

            TrackingReplyWriter rejecting = new TrackingReplyWriter();
            CapacityGate capacity = CapacityGate.rejecting();
            Assert.assertThrows(
                    ReplyCapacityUnavailableException.class,
                    () -> execute(processor, session, capacity, rejecting, cmd("EXEC"))
            );

            Assert.assertTrue(session.transaction().active());
            Assert.assertEquals(1, session.transaction().size());
            Assert.assertNull(db.reads().strings().getStringBytes(b("queued")));
            Assert.assertEquals(
                    REPLY_SIZER.plan(session, ReplyShapes.array(List.of(ReplyShapes.simpleString("OK")))),
                    capacity.reservedPlan()
            );
            session.discardTransaction();
        });
    }

    @Test
    public void execWithStateDependentQueuedReadReservesMaximumBeforeMutating() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            EngineSession session = new EngineSession(16, 16 * 1024L);
            TrackingReplyWriter accepting = new TrackingReplyWriter();

            execute(processor, session, accepting, cmd("MULTI"));
            execute(processor, session, accepting, cmd("SET", "queued", "value"));
            execute(processor, session, accepting, cmd("GET", "queued"));
            Assert.assertEquals(2, session.transaction().size());

            TrackingReplyWriter rejecting = new TrackingReplyWriter();
            CapacityGate capacity = CapacityGate.rejecting();
            Assert.assertThrows(
                    ReplyCapacityUnavailableException.class,
                    () -> execute(processor, session, capacity, rejecting, cmd("EXEC"))
            );

            Assert.assertEquals(ReplyPlan.maximum(), capacity.reservedPlan());
            Assert.assertEquals(2, session.transaction().size());
            Assert.assertNull(db.reads().strings().getStringBytes(b("queued")));
            Assert.assertEquals(0, rejecting.arrayHeaderCalls());
            session.discardTransaction();
        });
    }

    private static void assertScalarReplySource(
            YierdisFastCommandProcessor processor,
            List<byte[]> command,
            int payloadLength
    ) {
        EngineSession session = new EngineSession(16, 16 * 1024L);
        ReplyPlan plan = execute(processor, session, new TrackingReplyWriter(), command);
        Assert.assertEquals(
                REPLY_SIZER.plan(session, ReplyShapes.bulkString(payloadLength, plan.retainedSourceBytes())),
                plan
        );
    }

    private static void assertAggregatePreflight(
            YierdisFastCommandProcessor processor,
            List<byte[]> command,
            long expectedEncodedBytes
    ) {
        EngineSession session = new EngineSession(16, 16 * 1024L);
        CapacityGate capacity = CapacityGate.rejecting();
        Assert.assertThrows(
                ReplyCapacityUnavailableException.class,
                () -> execute(processor, session, capacity, new TrackingReplyWriter(), command)
        );
        Assert.assertEquals(expectedEncodedBytes, capacity.reservedPlan().encodedUpperBoundBytes());
        Assert.assertEquals(0L, capacity.reservedPlan().retainedSourceBytes());
    }

    private static ReplyPlan execute(
            YierdisFastCommandProcessor processor,
            EngineSession session,
            RedisReplyWriter writer,
            List<byte[]> command
    ) {
        return execute(processor, session, CapacityGate.accepting(), writer, ByteArrayExecutionRequest.copyOf(command));
    }

    private static ReplyPlan execute(
            YierdisFastCommandProcessor processor,
            EngineSession session,
            RedisReplyWriter writer,
            ExecutionRequest request
    ) {
        return execute(processor, session, CapacityGate.accepting(), writer, request);
    }

    private static ReplyPlan execute(
            YierdisFastCommandProcessor processor,
            EngineSession session,
            CapacityGate capacity,
            RedisReplyWriter writer,
            List<byte[]> command
    ) {
        return execute(processor, session, capacity, writer, ByteArrayExecutionRequest.copyOf(command));
    }

    private static ReplyPlan execute(
            YierdisFastCommandProcessor processor,
            EngineSession session,
            CapacityGate capacity,
            RedisReplyWriter writer,
            ExecutionRequest request
    ) {
        try {
            try (PreparedCommand prepared = processor.prepare(
                    request,
                    new CommandPreparationContext(session)
            )) {
                Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
                ReplyPlan plan = REPLY_SIZER.plan(session, prepared.replyShape());
                capacity.reserve(plan);
                try (CommandExecutionContext execution = CommandExecutionContext.forRequest(session, writer, request)) {
                    prepared.execute(execution);
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
