package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;

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
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 1_512));
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024, 16, 1_024, 1_024, budget, connection, RespDecodedMessageGate.PASS_THROUGH
        );
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            Assert.assertFalse(channel.writeInbound(unaccountedAscii("*1\r\n$32\r\n")));

            Assert.assertEquals("WAITING_FOR_BULK", decoder.stateNameForTests());
            Assert.assertEquals(0, decoder.allocatedBulkArraysForTests());
            Assert.assertEquals(1, budget.stats().waitingConnections());

            budget.release(blocker, 800);
            channel.runPendingTasks();

            Assert.assertEquals("READ_ARRAY_BODY", decoder.stateNameForTests());
            Assert.assertEquals(0, decoder.allocatedBulkArraysForTests());
        } finally {
            channel.finishAndReleaseAll();
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

            Assert.assertTrue(budget.stats().retainedInputCapacityBytes() > 0L);
            Assert.assertTrue(budget.stats().readCreditBytes() > 0L);
        } finally {
            channel.finishAndReleaseAll();
        }

        Assert.assertEquals(0L, budget.stats().readCreditBytes());
        Assert.assertEquals(0L, budget.stats().retainedInputCapacityBytes());
        Assert.assertEquals(0L, budget.stats().reservedBytes());
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

    private static io.netty.buffer.ByteBuf ascii(String value) {
        return Unpooled.copiedBuffer(value, StandardCharsets.US_ASCII);
    }

    private static AccountedInboundBuffer unaccountedAscii(String value) {
        return new AccountedInboundBuffer(ascii(value), InboundBufferLease.unaccounted());
    }

    private static byte[] asciiBytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
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
