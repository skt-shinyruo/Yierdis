package yier.bubu.redis.integration.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.resp.netty.InboundConnectionMemory;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudgetStats;
import yier.bubu.redis.protocol.resp.netty.RespDecodedMessageGate;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class RespIngressFuzzTest {
    private static final long GLOBAL_CAPACITY_BYTES = 1L << 20;
    private static final int CASES = 10_000;

    @Test
    public void fixedSeedProtocolFuzzReleasesEveryIngressReservation() {
        InboundMemoryBudget budget = new InboundMemoryBudget(GLOBAL_CAPACITY_BYTES);
        Random random = new Random(0x5EED_2026L);

        for (int i = 0; i < CASES; i++) {
            InboundConnectionMemory connection = new InboundConnectionMemory(
                    "fuzz-" + i,
                    65_536,
                    Runnable::run,
                    () -> { }
            );
            RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                    65_536,
                    16,
                    1_024,
                    65_536,
                    budget,
                    connection,
                    RespDecodedMessageGate.PASS_THROUGH
            );
            EmbeddedChannel channel = new EmbeddedChannel(decoder);
            try {
                writeFragmented(channel, frameFor(random, i), random);
                channel.runPendingTasks();
                drainRequests(channel);
            } finally {
                drainRequests(channel);
                channel.finishAndReleaseAll();
                connection.close();
            }
            assertZeroedAfterCase(budget);
        }
    }

    @Test
    public void oneHundredThousandOneByteFragmentsConsolidateWithoutLeaking() {
        InboundMemoryBudget budget = new InboundMemoryBudget(GLOBAL_CAPACITY_BYTES);
        InboundConnectionMemory connection = new InboundConnectionMemory(
                "one-byte",
                256 * 1024L,
                Runnable::run,
                () -> { }
        );
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                128 * 1024,
                16,
                1_024,
                128 * 1024,
                budget,
                connection,
                RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            byte[] frame = echoFrame(99_976);
            Assert.assertEquals(100_000, frame.length);
            for (byte value : frame) {
                channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{value}));
            }
            channel.runPendingTasks();

            Object message = channel.readInbound();
            Assert.assertTrue(message instanceof ExecutionRequest);
            try (ExecutionRequest request = (ExecutionRequest) message) {
                Assert.assertEquals(2, request.argc());
                Assert.assertEquals(99_976, request.len(1));
            }
            Assert.assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
            connection.close();
        }

        assertZeroedAfterCase(budget);
    }

    private static byte[] frameFor(Random random, int caseIndex) {
        return switch (random.nextInt(6)) {
            case 0 -> "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
            case 1 -> ("ECHO fuzz-" + caseIndex + "\r\n").getBytes(StandardCharsets.US_ASCII);
            case 2 -> "*x\r\n".getBytes(StandardCharsets.US_ASCII);
            case 3 -> "*1\r\n$4\r\nPING\rX".getBytes(StandardCharsets.US_ASCII);
            case 4 -> "*2\r\n$4\r\nPING\r\n$10\r\nincomplete".getBytes(StandardCharsets.US_ASCII);
            default -> "PING\r\n*1\r\n$-2\r\n".getBytes(StandardCharsets.US_ASCII);
        };
    }

    private static byte[] echoFrame(int bodyLength) {
        byte[] prefix = ("*2\r\n$4\r\nECHO\r\n$" + bodyLength + "\r\n").getBytes(StandardCharsets.US_ASCII);
        byte[] frame = new byte[prefix.length + bodyLength + 2];
        System.arraycopy(prefix, 0, frame, 0, prefix.length);
        for (int i = prefix.length; i < prefix.length + bodyLength; i++) {
            frame[i] = 'x';
        }
        frame[frame.length - 2] = '\r';
        frame[frame.length - 1] = '\n';
        return frame;
    }

    private static void writeFragmented(EmbeddedChannel channel, byte[] frame, Random random) {
        for (int offset = 0; offset < frame.length; ) {
            int length = Math.min(frame.length - offset, 1 + random.nextInt(8));
            byte[] fragment = new byte[length];
            System.arraycopy(frame, offset, fragment, 0, length);
            channel.writeInbound(Unpooled.wrappedBuffer(fragment));
            offset += length;
        }
    }

    private static void drainRequests(EmbeddedChannel channel) {
        Object message;
        while ((message = channel.readInbound()) != null) {
            if (message instanceof ExecutionRequest request) {
                request.close();
            }
        }
    }

    private static void assertZeroedAfterCase(InboundMemoryBudget budget) {
        InboundMemoryBudgetStats stats = budget.stats();
        Assert.assertEquals(0L, stats.reservedBytes());
        Assert.assertEquals(0L, stats.readCreditBytes());
        Assert.assertEquals(0L, stats.retainedInputCapacityBytes());
        Assert.assertEquals(0L, stats.consolidationBytes());
        Assert.assertEquals(0, stats.waitingConnections());
        Assert.assertFalse(stats.backpressured());
        Assert.assertTrue(stats.peakReservedBytes() <= GLOBAL_CAPACITY_BYTES);
    }
}
