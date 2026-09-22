package yier.bubu.redis.app.server;

// 连接关闭路径收敛回归（issue #32）：idle timeout 与慢客户端宽限关闭必须先标记 closing、
// 回收事务状态，再关闭 transport，而不是各自直接 ctx.close()。

import io.netty.buffer.Unpooled;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class UnifiedConnectionCloseTest {
    @Test
    public void idleTimeoutCloseMarksClosingAndDiscardsTransaction() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 4, 1024);
            ch.pipeline().addLast(new YierdisServerChannelInitializer.CloseOnReadIdleHandler());
            TransactionState tx = connection.session().transaction();
            tx.begin();
            Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
            Assert.assertEquals(1, tx.size());

            ch.pipeline().fireUserEventTriggered(IdleStateEvent.READER_IDLE_STATE_EVENT);

            Assert.assertTrue("idle close must mark closing", connection.context().isClosing());
            Assert.assertFalse("idle close must close the transport", ch.isOpen());
            Assert.assertFalse("idle close must discard transaction state", tx.active());
            Assert.assertEquals(0, tx.size());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void idleHandlerIgnoresNonReaderIdleEvents() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            NettyExecutionConnection.getOrCreate(ch, 4, 1024);
            ch.pipeline().addLast(new YierdisServerChannelInitializer.CloseOnReadIdleHandler());

            ch.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);

            Assert.assertTrue("writer idle event must not close the channel", ch.isOpen());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void slowClientGraceCloseMarksClosingAndDiscardsTransaction() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        try {
            // executor 不启动：handler 只会回调 onTransportUnwritable，该路径不依赖 drain loop。
            CommandExecutor executor = new CommandExecutor(
                    () -> { },
                    (session, request) -> { throw new UnsupportedOperationException(); },
                    new NettySerialOwnerExecutor(group.next()),
                    (version, shape) -> { throw new UnsupportedOperationException(); },
                    (version, sink) -> { throw new UnsupportedOperationException(); },
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            EmbeddedChannel ch = new EmbeddedChannel();
            try {
                NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 4, 1024);
                ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 8));
                ch.pipeline().addLast(new YierdisServerChannelInitializer.WriteBufferBackpressureHandler(executor, 10));
                TransactionState tx = connection.session().transaction();
                tx.begin();
                Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
                Assert.assertEquals(1, tx.size());

                // 只 write 不 flush：出站字节滞留在 outbound buffer，水位持续超 high watermark。
                ch.write(Unpooled.buffer(64, 64));
                Assert.assertFalse("expected channel to be unwritable above the high watermark", ch.isWritable());

                // writability 事件先以普通任务投递到 event loop，handler 收到后才安排宽限关闭任务。
                ch.runPendingTasks();
                // EmbeddedEventLoop 默认跟随真实时间，手动推进到宽限期之后触发慢客户端关闭。
                ch.advanceTimeBy(1, TimeUnit.SECONDS);
                ch.runScheduledPendingTasks();

                Assert.assertTrue("slow-client close must mark closing", connection.context().isClosing());
                Assert.assertFalse("slow-client close must close the transport", ch.isOpen());
                Assert.assertFalse("slow-client close must discard transaction state", tx.active());
                Assert.assertEquals(0, tx.size());
            } finally {
                ch.finishAndReleaseAll();
            }
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], Arrays.asList(Arrays.copyOfRange(args, 1, args.length)));
    }
}
