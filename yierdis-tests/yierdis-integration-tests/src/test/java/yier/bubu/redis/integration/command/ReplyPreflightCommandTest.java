package yier.bubu.redis.integration.command;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.CommandSessionCapabilities;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ReplyPreflightCommandTest {
    @Test
    public void setGetCapacityRejectionLeavesTheExistingValueUntouched() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "key", "old"));

                TrackingReplyWriter writer = TrackingReplyWriter.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, new EngineSession(16, 16 * 1024L), writer, cmd("SET", "key", "new", "GET"))
                );

                Assert.assertArrayEquals(b("old"), db.reads().strings().getStringBytes(b("key")));
                Assert.assertEquals(ReplyPlans.bulkString(3, 0L).encodedUpperBoundBytes(), writer.requiredPlan().encodedUpperBoundBytes());
            }
        });
    }

    @Test
    public void countedPopCapacityRejectionLeavesTheListUntouched() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("RPUSH", "list", "a", "bb"));

                TrackingReplyWriter writer = TrackingReplyWriter.rejecting();
                Assert.assertThrows(
                        ReplyCapacityUnavailableException.class,
                        () -> execute(processor, new EngineSession(16, 16 * 1024L), writer, cmd("LPOP", "list", "2"))
                );

                Assert.assertEquals(2, db.reads().lists().lrange(b("list"), 0, -1).count());
                Assert.assertEquals(19L, writer.requiredPlan().encodedUpperBoundBytes());
            }
        });
    }

    @Test
    public void getHgetAndEchoPreflightAndTransferTheirReplySources() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "string", "value"));
                client.execute(cmd("HSET", "hash", "field", "value"));

                assertScalarReplySource(processor, cmd("GET", "string"), 5);
                assertScalarReplySource(processor, cmd("HGET", "hash", "field"), 5);

                ExecutionRequest echo = ByteArrayExecutionRequest.fromUtf8("ECHO", List.of("message"));
                TrackingReplyWriter echoWriter = TrackingReplyWriter.accepting();
                try {
                    execute(processor, new EngineSession(16, 16 * 1024L), echoWriter, echo);
                    Assert.assertEquals(ReplyPlans.bulkString(7, echo.admittedMemoryBytes()), echoWriter.requiredPlan());
                    Assert.assertEquals(1, echoWriter.transferredResources().size());
                    Assert.assertTrue(echoWriter.transferredResources().get(0) instanceof ExecutionRequest);
                } finally {
                    echoWriter.closeTransferredResources();
                }
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
    public void execCapacityRejectionDoesNotDrainOrExecuteTheQueuedTransaction() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            EngineSession session = new EngineSession(16, 16 * 1024L);
            TrackingReplyWriter accepting = TrackingReplyWriter.accepting();

            execute(processor, session, accepting, cmd("MULTI"));
            execute(processor, session, accepting, cmd("SET", "queued", "value"));
            Assert.assertTrue(session.transaction().active());
            Assert.assertEquals(1, session.transaction().size());

            TrackingReplyWriter rejecting = TrackingReplyWriter.rejecting();
            Assert.assertThrows(
                    ReplyCapacityUnavailableException.class,
                    () -> execute(processor, session, rejecting, cmd("EXEC"))
            );

            Assert.assertTrue(session.transaction().active());
            Assert.assertEquals(1, session.transaction().size());
            Assert.assertNull(db.reads().strings().getStringBytes(b("queued")));
            Assert.assertEquals(ReplyPlan.maximum(), rejecting.requiredPlan());
            session.discardTransaction();
        });
    }

    private static void assertScalarReplySource(
            YierdisFastCommandProcessor processor,
            List<byte[]> command,
            int payloadLength
    ) {
        TrackingReplyWriter writer = TrackingReplyWriter.accepting();
        try {
            execute(processor, new EngineSession(16, 16 * 1024L), writer, command);
            Assert.assertEquals(ReplyPlans.bulkString(payloadLength, writer.requiredPlan().retainedSourceBytes()).encodedUpperBoundBytes(),
                    writer.requiredPlan().encodedUpperBoundBytes());
            Assert.assertEquals(1, writer.transferredResources().size());
        } finally {
            writer.closeTransferredResources();
        }
    }

    private static void assertAggregatePreflight(
            YierdisFastCommandProcessor processor,
            List<byte[]> command,
            long expectedEncodedBytes
    ) {
        TrackingReplyWriter writer = TrackingReplyWriter.rejecting();
        Assert.assertThrows(
                ReplyCapacityUnavailableException.class,
                () -> execute(processor, new EngineSession(16, 16 * 1024L), writer, command)
        );
        Assert.assertEquals(expectedEncodedBytes, writer.requiredPlan().encodedUpperBoundBytes());
        Assert.assertEquals(0L, writer.requiredPlan().retainedSourceBytes());
    }

    private static void execute(
            YierdisFastCommandProcessor processor,
            EngineSession session,
            RedisReplyWriter writer,
            List<byte[]> command
    ) {
        execute(processor, session, writer, ByteArrayExecutionRequest.copyOf(command));
    }

    private static void execute(
            YierdisFastCommandProcessor processor,
            EngineSession session,
            RedisReplyWriter writer,
            ExecutionRequest request
    ) {
        try {
            processor.execute(request, new CommandContext(CommandSessionCapabilities.from(session), writer));
        } finally {
            request.close();
        }
    }

    private static final class TrackingReplyWriter implements RedisReplyWriter {
        private final boolean rejectCapacity;
        private final List<ReplyPlan> requiredPlans = new ArrayList<>();
        private final List<AutoCloseable> transferredResources = new ArrayList<>();
        private boolean closeAfterReply;

        private TrackingReplyWriter(boolean rejectCapacity) {
            this.rejectCapacity = rejectCapacity;
        }

        static TrackingReplyWriter accepting() {
            return new TrackingReplyWriter(false);
        }

        static TrackingReplyWriter rejecting() {
            return new TrackingReplyWriter(true);
        }

        ReplyPlan requiredPlan() {
            Assert.assertEquals("expected exactly one reply preflight", 1, requiredPlans.size());
            return requiredPlans.get(0);
        }

        List<AutoCloseable> transferredResources() {
            return transferredResources;
        }

        void closeTransferredResources() {
            for (AutoCloseable resource : transferredResources) {
                try {
                    resource.close();
                } catch (Exception failure) {
                    throw new AssertionError("reply source close failed", failure);
                }
            }
            transferredResources.clear();
        }

        @Override
        public void requireReply(ReplyPlan plan) {
            requiredPlans.add(plan);
            if (rejectCapacity) {
                throw new ReplyCapacityUnavailableException("injected reply capacity rejection");
            }
        }

        @Override
        public void transferReplyOwnership(AutoCloseable resource) {
            transferredResources.add(resource);
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
        }

        @Override
        public void emptyArray() {
        }

        @Override
        public void mapHeader(int pairs) {
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
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
        }

        @Override
        public void bulkString(BytesSlice slice) {
        }

        @Override
        public void bulkStringLongAscii(long value) {
        }
    }
}
