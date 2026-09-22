package yier.bubu.redis.execution.executor;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.protocol.resp.RespReplyWriter;

/**
 * 协议版本在 prepare/预留时刻读取一次并被捕获进 {@link ReplyPlan}，writer 按同一份捕获值渲染。
 * 这组测试守住：无论 execute 期 session 版本如何变化，预留字节数与实际写出字节数始终一致。
 */
public class ReplyProtocolVersionCaptureTest {

    @Test
    public void declaredVersionDrivesSizingAndRenderingWhenExecuteSwitchesToResp3() throws Exception {
        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        RedisReply reply = wideMapReply();

        PlanRecordingReply result = runCommand(
                (session, request) -> PreparedCommands.action(reply.shape(), 3, execution -> {
                    execution.setRespVersion(3);
                    return CommandResult.reply(reply);
                }),
                connection
        );

        Assert.assertTrue(result.ready());
        Assert.assertEquals(3, result.reservedPlan().protocolVersion());
        Assert.assertEquals(
                "reserved bytes must equal written bytes",
                result.reservedPlan().encodedUpperBoundBytes(),
                result.writtenBytes().length
        );
        Assert.assertTrue(
                "reply must render as a RESP3 map: " + result.writtenAscii(),
                result.writtenAscii().startsWith("%8\r\n")
        );
        Assert.assertEquals(3, connection.session().respVersion());
    }

    @Test
    public void resp2RenderingLongerThanSessionResp3StillFitsReservation() throws Exception {
        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        connection.session().setRespVersion(3);
        // null 与 map 头的 RESP2 编码都比 RESP3 长：按声明版本 2 预留必须覆盖更长的实际写出。
        RedisReply reply = RedisReplies.array(List.of(RedisReplies.nullValue(), wideMapReply()));

        PlanRecordingReply result = runCommand(
                (session, request) -> PreparedCommands.action(reply.shape(), 2, execution -> {
                    execution.setRespVersion(2);
                    return CommandResult.reply(reply);
                }),
                connection
        );

        Assert.assertTrue(result.ready());
        Assert.assertEquals(2, result.reservedPlan().protocolVersion());
        Assert.assertEquals(
                "reserved bytes must equal written bytes",
                result.reservedPlan().encodedUpperBoundBytes(),
                result.writtenBytes().length
        );
        Assert.assertEquals(
                "*2\r\n$-1\r\n*16\r\n"
                        + "$2\r\nk0\r\n:0\r\n$2\r\nk1\r\n:1\r\n"
                        + "$2\r\nk2\r\n:2\r\n$2\r\nk3\r\n:3\r\n"
                        + "$2\r\nk4\r\n:4\r\n$2\r\nk5\r\n:5\r\n"
                        + "$2\r\nk6\r\n:6\r\n$2\r\nk7\r\n:7\r\n",
                result.writtenAscii()
        );
        Assert.assertEquals(2, connection.session().respVersion());
    }

    @Test
    public void undeclaredVersionCapturesSessionVersionAtPrepareTime() throws Exception {
        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        RedisReply reply = RedisReplies.nullValue();

        PlanRecordingReply result = runCommand(
                (session, request) -> PreparedCommands.action(reply.shape(), execution -> {
                    // 未声明版本的命令即使在 execute 期改动 session，本次回复仍按 prepare 捕获值渲染。
                    execution.setRespVersion(3);
                    return CommandResult.reply(reply);
                }),
                connection
        );

        Assert.assertTrue(result.ready());
        Assert.assertEquals(2, result.reservedPlan().protocolVersion());
        Assert.assertEquals(
                "reserved bytes must equal written bytes",
                result.reservedPlan().encodedUpperBoundBytes(),
                result.writtenBytes().length
        );
        Assert.assertEquals("$-1\r\n", result.writtenAscii());
        Assert.assertEquals(3, connection.session().respVersion());
    }

    private static RedisReply wideMapReply() {
        List<RedisReply> elements = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            elements.add(RedisReplies.bulkString(("k" + i).getBytes(StandardCharsets.US_ASCII)));
            elements.add(RedisReplies.integer(i));
        }
        return RedisReplies.map(elements);
    }

    private static PlanRecordingReply runCommand(
            BiFunction<CommandSession, ExecutionRequest, PreparedCommand> engine,
            TestConnection connection
    ) throws Exception {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        CommandExecutor executor = new CommandExecutor(
                () -> { },
                engine,
                ownerExecutor,
                new RespReplySizer(),
                RespReplyWriter::new,
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);
        try {
            PlanRecordingReply reply = new PlanRecordingReply();
            ExecutorCoreTestSupport.publish(
                    executor,
                    connection,
                    TrackingExecutionRequest.ofUtf8("TEST"),
                    reply
            );
            ownerExecutor.runAll();
            return reply;
        } finally {
            // shutdown 的完成回调也排在 owner executor 队列里，必须先 drain 再等待。
            CompletableFuture<Void> shutdown = executor.shutdownGracefully();
            ownerExecutor.runAll();
            shutdown.get(1, TimeUnit.SECONDS);
        }
    }

    private static final class PlanRecordingReply implements ExecutionReply {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private ReplyPlan reservedPlan;
        private boolean ready;

        @Override
        public ReplyReservationResult tryReserve(ReplyPlan plan) {
            reservedPlan = Objects.requireNonNull(plan, "plan");
            return ReplyReservationResult.RESERVED;
        }

        @Override
        public Runnable onCapacityAvailable(Runnable wakeup) {
            return null;
        }

        @Override
        public BytesSink sink() {
            return bytes::write;
        }

        @Override
        public void markReady(boolean closeAfterReply) {
            ready = true;
        }

        @Override
        public void cancel() {
        }

        @Override
        public boolean hasWrittenBytes() {
            return bytes.size() > 0;
        }

        @Override
        public void markResultUnknown() {
        }

        ReplyPlan reservedPlan() {
            return reservedPlan;
        }

        byte[] writtenBytes() {
            return bytes.toByteArray();
        }

        String writtenAscii() {
            return bytes.toString(StandardCharsets.US_ASCII);
        }

        boolean ready() {
            return ready;
        }
    }
}
