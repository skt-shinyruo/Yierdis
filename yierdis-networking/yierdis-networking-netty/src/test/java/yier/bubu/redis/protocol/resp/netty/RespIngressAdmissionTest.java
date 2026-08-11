package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RespIngressAdmissionTest {
    @Test
    public void argvIsRejectedBeforeOuterArrayAllocation() {
        InboundMemoryBudget budget = new InboundMemoryBudget(4_096);
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 80, Runnable::run, () -> { });
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 1_000_000, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            Assert.assertTrue(channel.writeInbound(ascii("*1000000\r\n")));

            Object message = channel.readInbound();
            Assert.assertTrue(message instanceof RespProtocolError);
            Assert.assertEquals("ERR request exceeds configured memory limit", ((RespProtocolError) message).message());
            Assert.assertEquals(0, decoder.allocatedArgvArraysForTests());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void declaredBulkWaitsWithoutAllocatingBodyArray() {
        InboundMemoryBudget budget = new InboundMemoryBudget(2_048);
        InboundConnectionMemory blocker = new InboundConnectionMemory("blocker", 2_048, Runnable::run, () -> { });
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 2_048, Runnable::run, () -> { });
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 1_000));
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            Assert.assertFalse(channel.writeInbound(unaccountedAscii("*1\r\n$1024\r\n")));

            Assert.assertEquals("WAITING_FOR_BULK", decoder.stateNameForTests());
            Assert.assertEquals(0, decoder.allocatedBulkArraysForTests());
            Assert.assertEquals(1, budget.stats().waitingConnections());

            budget.release(blocker, 800);
            channel.runPendingTasks();

            Assert.assertEquals("READ_ARRAY_BODY", decoder.stateNameForTests());
            Assert.assertEquals(0, decoder.allocatedBulkArraysForTests());
        } finally {
            channel.finishAndReleaseAll();
            budget.release(blocker, 200);
        }
    }

    @Test
    public void consolidationReservationIsConsumedExactlyOnceAfterWait() {
        InboundMemoryBudget budget = new InboundMemoryBudget(4_096);
        InboundConnectionMemory blocker = new InboundConnectionMemory("blocker", 4_096, Runnable::run, () -> { });
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 4_096, Runnable::run, () -> { });
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 4_000));
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            for (int i = 0; i < 16; i++) {
                Assert.assertFalse(channel.writeInbound(unaccountedAscii("a")));
            }
            Assert.assertEquals(1, budget.stats().waitingConnections());

            budget.release(blocker, 2_000);
            channel.runPendingTasks();
        } finally {
            channel.finishAndReleaseAll();
            budget.release(blocker, 2_000);
        }

        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void fragmentedInputIsAccountedAndReleasedOnDisconnect() {
        InboundMemoryBudget budget = new InboundMemoryBudget(4_096);
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 4_096, Runnable::run, () -> { });
        InboundReadCreditHandler readCredits = new InboundReadCreditHandler(budget, connection, 128);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        decoder.setReadControl(readCredits);
        EmbeddedChannel channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        try {
            channel.runPendingTasks();
            Assert.assertFalse(channel.config().isAutoRead());
            Assert.assertTrue(readCredits.outstandingReadCreditBytes() > 0L);

            Assert.assertFalse(channel.writeInbound(ascii("*1\r\n$4\r\nPI")));
            channel.runPendingTasks();

            Assert.assertEquals(0L, budget.stats().retainedInputCapacityBytes());
            Assert.assertTrue(budget.stats().reservedBytes() > budget.stats().readCreditBytes());
            Assert.assertTrue(budget.stats().readCreditBytes() > 0L);
        } finally {
            channel.finishAndReleaseAll();
        }

        Assert.assertEquals(0L, budget.stats().readCreditBytes());
        Assert.assertEquals(0L, budget.stats().retainedInputCapacityBytes());
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void fragmentedBulkBodyReleasesConsumedInputAndCompletesWithinTheConnectionLimit() {
        InboundMemoryBudget budget = new InboundMemoryBudget(600);
        InboundConnectionMemory connection = new InboundConnectionMemory("streaming-bulk", 480, Runnable::run, () -> { });
        InboundReadCreditHandler readCredits = new InboundReadCreditHandler(budget, connection, 64);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                512, 4, 1_024, 512, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        decoder.setReadControl(readCredits);
        EmbeddedChannel channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        ExecutionRequest request = null;
        try {
            channel.runPendingTasks();
            Assert.assertFalse(channel.writeInbound(ascii("*1\r\n$256\r\n")));
            channel.runPendingTasks();
            Assert.assertNull(channel.readInbound());

            byte[] fragment = new byte[32];
            Arrays.fill(fragment, (byte) 'x');
            for (int index = 0; index < 7; index++) {
                Assert.assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(fragment.clone())));
                channel.runPendingTasks();
                Assert.assertNull(channel.readInbound());
                Assert.assertTrue(connection.reservedBytes() <= 480L);
            }

            byte[] finalFragment = new byte[34];
            Arrays.fill(finalFragment, 0, 32, (byte) 'x');
            finalFragment[32] = '\r';
            finalFragment[33] = '\n';
            Assert.assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(finalFragment)));
            channel.runPendingTasks();

            Object decoded = channel.readInbound();
            Assert.assertTrue(decoded instanceof ExecutionRequest);
            request = (ExecutionRequest) decoded;
            Assert.assertEquals(1, request.argc());
            Assert.assertArrayEquals(repeatedByte('x', 256), request.readOnlyByteArray(0));
            Assert.assertNull(channel.readInbound());
        } finally {
            if (request != null) {
                request.close();
            }
            channel.finishAndReleaseAll();
            connection.close();
        }

        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void fragmentedBulkContinuesAfterItsPayloadReservationCrossesTheHighWatermark() {
        InboundMemoryBudget budget = new InboundMemoryBudget(600);
        InboundConnectionMemory connection = new InboundConnectionMemory(
                "high-water-progress", 600, Runnable::run, () -> { });
        InboundReadCreditHandler readCredits = new InboundReadCreditHandler(budget, connection, 64);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                512, 4, 1_024, 512, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        decoder.setReadControl(readCredits);
        EmbeddedChannel channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        ExecutionRequest request = null;
        try {
            channel.runPendingTasks();
            Assert.assertFalse(channel.writeInbound(ascii("*1\r\n$420\r\n")));
            channel.runPendingTasks();

            Assert.assertTrue(budget.stats().backpressured());
            Assert.assertTrue(readCredits.outstandingReadCreditBytes() > 0L);
            Assert.assertEquals(0, budget.stats().waitingConnections());

            byte[] fragment = new byte[64];
            Arrays.fill(fragment, (byte) 'x');
            for (int index = 0; index < 6; index++) {
                Assert.assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(fragment.clone())));
                channel.runPendingTasks();
                Assert.assertNull(channel.readInbound());
            }

            byte[] finalFragment = new byte[38];
            Arrays.fill(finalFragment, 0, 36, (byte) 'x');
            finalFragment[36] = '\r';
            finalFragment[37] = '\n';
            Assert.assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(finalFragment)));
            channel.runPendingTasks();

            request = (ExecutionRequest) channel.readInbound();
            Assert.assertNotNull(request);
            Assert.assertArrayEquals(repeatedByte('x', 420), request.readOnlyByteArray(0));
        } finally {
            if (request != null) {
                request.close();
            }
            channel.finishAndReleaseAll();
            connection.close();
        }

        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void completedRequestMemoryDoesNotGrantProgressCreditToTheNextRequest() {
        InboundMemoryBudget budget = new InboundMemoryBudget(500);
        InboundConnectionMemory blocker = new InboundConnectionMemory("blocker", 500, Runnable::run, () -> { });
        InboundConnectionMemory connection = new InboundConnectionMemory("completed", 500, Runnable::run, () -> { });
        InboundReadCreditHandler readCredits = new InboundReadCreditHandler(budget, connection, 64);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                256, 4, 1_024, 256, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        decoder.setReadControl(readCredits);
        EmbeddedChannel channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        ExecutionRequest request = null;
        try {
            channel.runPendingTasks();
            Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 247));
            Assert.assertTrue(budget.stats().backpressured());

            Assert.assertTrue(channel.writeInbound(ascii("*1\r\n$4\r\nPING\r\n")));
            channel.runPendingTasks();
            request = (ExecutionRequest) channel.readInbound();

            Assert.assertEquals(1, budget.stats().waitingConnections());
            Assert.assertEquals(0L, readCredits.outstandingReadCreditBytes());
        } finally {
            if (request != null) {
                request.close();
            }
            channel.finishAndReleaseAll();
            budget.release(blocker, 247);
        }
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void impossibleInitialReadCreditProducesATerminalMemoryError() {
        InboundMemoryBudget budget = new InboundMemoryBudget(64);
        InboundConnectionMemory connection = new InboundConnectionMemory("read-credit", 64, Runnable::run, () -> { });
        InboundReadCreditHandler readCredits = new InboundReadCreditHandler(budget, connection, 64);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        decoder.setReadControl(readCredits);
        EmbeddedChannel channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        try {
            channel.runPendingTasks();

            Object message = channel.readInbound();
            Assert.assertTrue(message instanceof RespProtocolError);
            Assert.assertEquals("ERR request exceeds configured memory limit", ((RespProtocolError) message).message());
            Assert.assertTrue(((RespProtocolError) message).closeAfterReply());
            Assert.assertEquals("CLOSING", decoder.stateNameForTests());
            Assert.assertEquals(0, budget.stats().waitingConnections());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
            connection.close();
        }
    }

    @Test
    public void unreservableInboundBufferIsRejected() {
        InboundMemoryBudget budget = new InboundMemoryBudget(200);
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 200, Runnable::run, () -> { });
        InboundReadCreditHandler readCredits = new InboundReadCreditHandler(budget, connection, 32);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        decoder.setReadControl(readCredits);
        EmbeddedChannel channel = new EmbeddedChannel(readCredits, new InboundByteAccountingHandler(readCredits), decoder);
        try {
            channel.runPendingTasks();
            io.netty.buffer.ByteBuf input = Unpooled.buffer(256);
            input.writeByte('a');

            Assert.assertTrue(channel.writeInbound(input));
            Object message = channel.readInbound();
            Assert.assertTrue(message instanceof RespProtocolError);
            Assert.assertEquals("ERR request exceeds configured memory limit", ((RespProtocolError) message).message());
            Assert.assertEquals(0L, budget.stats().readCreditBytes());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void inlineCommandWaitsForAdmissionBeforeParsing() {
        InboundMemoryBudget budget = new InboundMemoryBudget(2_048);
        InboundConnectionMemory blocker = new InboundConnectionMemory("blocker", 2_048, Runnable::run, () -> { });
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 2_048, Runnable::run, () -> { });
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 1_536));
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        ExecutionRequest request = null;
        try {
            Assert.assertFalse(channel.writeInbound(unaccountedAscii("PING\r\n")));
            Assert.assertEquals(1, budget.stats().waitingConnections());
            Assert.assertNull(channel.readInbound());

            budget.release(blocker, 800);
            channel.runPendingTasks();

            Object message = channel.readInbound();
            Assert.assertTrue(message instanceof ExecutionRequest);
            request = (ExecutionRequest) message;
            Assert.assertArrayEquals(asciiBytes("PING"), request.readOnlyByteArray(0));
        } finally {
            if (request != null) {
                request.close();
            }
            channel.finishAndReleaseAll();
            budget.release(blocker, 736);
        }

        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void inlineCommandOverAdmissionLimitProducesTerminalMemoryError() {
        InboundMemoryBudget budget = new InboundMemoryBudget(512);
        InboundConnectionMemory connection = new InboundConnectionMemory("decoder", 512, Runnable::run, () -> { });
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            Assert.assertTrue(channel.writeInbound(unaccountedAscii("PING\r\n")));

            Object message = channel.readInbound();
            Assert.assertTrue(message instanceof RespProtocolError);
            Assert.assertEquals("ERR request exceeds configured memory limit", ((RespProtocolError) message).message());
            Assert.assertEquals("CLOSING", decoder.stateNameForTests());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void decodedRequestWaitsAtGateWithoutEmittingLaterPipelineMessages() {
        RecordingGate gate = new RecordingGate();
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, null, null, gate
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            Assert.assertFalse(channel.writeInbound(ascii("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")));

            Assert.assertEquals("WAITING_FOR_HANDOFF", decoder.stateNameForTests());
            Assert.assertEquals(1, gate.attempts);
            Assert.assertNull(channel.readInbound());

            gate.admit = true;
            gate.resume.run();
            channel.runPendingTasks();

            Object first = channel.readInbound();
            Assert.assertTrue(first instanceof ExecutionRequest);
            ((ExecutionRequest) first).close();
            Assert.assertEquals(3, gate.attempts);
            Assert.assertEquals("WAITING_FOR_HANDOFF", decoder.stateNameForTests());
            Assert.assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void ingressConstructorRequiresBudgetAndConnectionTogether() {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory connection = new InboundConnectionMemory(
                "decoder", 1_024, Runnable::run, () -> { }
        );

        Assert.assertThrows(IllegalArgumentException.class, () -> RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, null, RespDecodedMessageGate.PASS_THROUGH
        ));
        Assert.assertThrows(IllegalArgumentException.class, () -> RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, null, connection, RespDecodedMessageGate.PASS_THROUGH
        ));
        Assert.assertThrows(NullPointerException.class,
                () -> new RespDecodedMessageGate.Admission(null, null));
        Assert.assertThrows(NullPointerException.class,
                () -> new RespDecodedMessageGate.Admission(
                        RespDecodedMessageGate.Status.ADMITTED,
                        null
                ));
    }

    @Test
    public void closedNullAndThrowingGatesReleaseDecodedRequestsAndIncomingBuffers() {
        assertTerminalGateOutcome((ctx, decoded, resume) -> RespDecodedMessageGate.Admission.closed());
        assertTerminalGateOutcome((ctx, decoded, resume) -> null);
        assertTerminalGateOutcome((ctx, decoded, resume) -> {
            throw new IllegalStateException("gate failed");
        });
    }

    @Test
    public void removingDecoderWhileGateWaitsReleasesThePendingRequestLease() {
        InboundMemoryBudget budget = new InboundMemoryBudget(4_096);
        InboundConnectionMemory connection = new InboundConnectionMemory(
                "waiting-gate", 4_096, Runnable::run, () -> { }
        );
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024,
                16,
                1_024,
                1_024,
                budget,
                connection,
                (ctx, decoded, resume) -> RespDecodedMessageGate.Admission.waiting()
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);

        Assert.assertFalse(channel.writeInbound(unaccountedAscii("PING\r\n")));
        Assert.assertEquals("WAITING_FOR_HANDOFF", decoder.stateNameForTests());
        Assert.assertTrue(budget.stats().reservedBytes() > 0L);

        channel.finishAndReleaseAll();

        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    private static void assertTerminalGateOutcome(RespDecodedMessageGate gate) {
        InboundMemoryBudget budget = new InboundMemoryBudget(4_096);
        InboundConnectionMemory connection = new InboundConnectionMemory(
                "terminal-gate", 4_096, Runnable::run, () -> { }
        );
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, gate
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        io.netty.buffer.ByteBuf input = ascii("PING\r\n");
        try {
            Assert.assertFalse(channel.writeInbound(new AccountedInboundBuffer(
                    input,
                    InboundBufferLease.unaccounted()
            )));
            Assert.assertEquals("CLOSING", decoder.stateNameForTests());
            Assert.assertEquals(0, input.refCnt());
            Assert.assertEquals(0L, budget.stats().reservedBytes());

            io.netty.buffer.ByteBuf late = ascii("PING\r\n");
            Assert.assertFalse(channel.writeInbound(late));
            Assert.assertEquals(0, late.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    private static io.netty.buffer.ByteBuf ascii(String value) {
        return Unpooled.copiedBuffer(value, StandardCharsets.US_ASCII);
    }

    private static AccountedInboundBuffer unaccountedAscii(String value) {
        return new AccountedInboundBuffer(ascii(value), InboundBufferLease.unaccounted());
    }

    private static byte[] asciiBytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] repeatedByte(char value, int length) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static final class RecordingGate implements RespDecodedMessageGate {
        private int attempts;
        private boolean admit;
        private Runnable resume;

        @Override
        public Admission tryAdmit(ChannelHandlerContext ctx, Object decoded, Runnable resumeOnEventLoop) {
            attempts++;
            if (admit) {
                admit = false;
                return Admission.admitted(decoded);
            }
            resume = resumeOnEventLoop;
            return Admission.waiting();
        }
    }
}
