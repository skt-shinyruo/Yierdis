package yier.bubu.redis.integration.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.resp.netty.InboundByteAccountingHandler;
import yier.bubu.redis.protocol.resp.netty.InboundConnectionMemory;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudgetStats;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespDecodedMessageGate;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespIngressPressureTest {
    private static final long GLOBAL_CAPACITY_BYTES = 1L << 20;
    private static final int CONNECTIONS = 32;
    private static final int RECEIVE_BUFFER_BYTES = 1_024;
    private static final long RECEIVE_CREDIT_BYTES = RECEIVE_BUFFER_BYTES + 64L;

    @Test
    public void pressureAcrossThirtyTwoCreditedConnectionsReleasesAllIngressCapacity() {
        InboundMemoryBudget budget = new InboundMemoryBudget(GLOBAL_CAPACITY_BYTES);
        List<IngressFixture> connections = new ArrayList<>();
        try {
            for (int i = 0; i < CONNECTIONS; i++) {
                IngressFixture fixture = new IngressFixture(budget, "pressure-" + i);
                connections.add(fixture);
                fixture.channel.runPendingTasks();

                Assert.assertTrue("each application read must hold at most one fixed credit",
                        fixture.readCredits.outstandingReadCreditBytes() <= RECEIVE_CREDIT_BYTES);
                fixture.write("*1\r\n$32768\r\n");

                InboundMemoryBudgetStats stats = budget.stats();
                Assert.assertTrue("admission must never oversubscribe the global budget",
                        stats.reservedBytes() <= GLOBAL_CAPACITY_BYTES);
                Assert.assertTrue("peak must remain bounded by the configured capacity",
                        stats.peakReservedBytes() <= GLOBAL_CAPACITY_BYTES);
                Assert.assertTrue("a connection cannot have two outstanding read credits",
                        fixture.readCredits.outstandingReadCreditBytes() <= RECEIVE_CREDIT_BYTES);
            }

            InboundMemoryBudgetStats pressured = budget.stats();
            Assert.assertTrue("declared bulks must engage global pressure", pressured.backpressured());
            Assert.assertTrue("at least one connection must be queued at global pressure",
                    pressured.waitingConnections() > 0);
            Assert.assertTrue("at least one queued connection must pause ingress",
                    connections.stream().anyMatch(connection -> connection.readCredits.inputPausedByIngress()));
        } finally {
            for (IngressFixture fixture : connections) {
                fixture.close();
            }
        }

        assertFullyReleased(budget);

        IngressFixture ping = new IngressFixture(budget, "after-pressure");
        try {
            ping.write("*1\r\n$4\r\nPING\r\n");
            Object message = ping.channel.readInbound();
            Assert.assertTrue("ingress must resume after disconnected pressure clients release", message instanceof ExecutionRequest);
            try (ExecutionRequest request = (ExecutionRequest) message) {
                Assert.assertArrayEquals("PING".getBytes(StandardCharsets.US_ASCII), request.readOnlyByteArray(0));
            }
        } finally {
            ping.close();
        }

        assertFullyReleased(budget);
    }

    private static void assertFullyReleased(InboundMemoryBudget budget) {
        InboundMemoryBudgetStats stats = budget.stats();
        Assert.assertEquals(0L, stats.reservedBytes());
        Assert.assertEquals(0L, stats.readCreditBytes());
        Assert.assertEquals(0L, stats.retainedInputCapacityBytes());
        Assert.assertEquals(0L, stats.consolidationBytes());
        Assert.assertEquals(0, stats.waitingConnections());
        Assert.assertFalse(stats.backpressured());
        Assert.assertTrue(stats.peakReservedBytes() <= GLOBAL_CAPACITY_BYTES);
    }

    private static final class IngressFixture implements AutoCloseable {
        private final InboundConnectionMemory memory;
        private final InboundReadCreditHandler readCredits;
        private final EmbeddedChannel channel;

        private IngressFixture(InboundMemoryBudget budget, String id) {
            memory = new InboundConnectionMemory(id, 65_536, Runnable::run, () -> { });
            readCredits = new InboundReadCreditHandler(budget, memory, RECEIVE_BUFFER_BYTES);
            RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                    65_536,
                    16,
                    1_024,
                    65_536,
                    budget,
                    memory,
                    RespDecodedMessageGate.PASS_THROUGH
            );
            decoder.setReadControl(readCredits);
            channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        }

        private void write(String frame) {
            channel.writeInbound(Unpooled.copiedBuffer(frame, StandardCharsets.US_ASCII));
            channel.runPendingTasks();
        }

        @Override
        public void close() {
            Object message;
            while ((message = channel.readInbound()) != null) {
                if (message instanceof ExecutionRequest request) {
                    request.close();
                }
            }
            channel.finishAndReleaseAll();
            memory.close();
        }
    }
}
