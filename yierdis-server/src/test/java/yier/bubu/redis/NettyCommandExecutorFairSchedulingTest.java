package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.netty.NettyRespFrame;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NettyCommandExecutorFairSchedulingTest {
    @Test
    public void fairSchedulingProcessesDifferentChannelsWithinSameTick() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                processor,
                eventExecutor,
                1024,
                0,
                256,
                128,
                0,
                0,
                2,
                1000,
                NettyCommandExecutor.SchedulingPolicy.FAIR,
                0,
                2.0,
                1024 * 1024
        );
        executor.start();

        // Block the executor thread so we can observe the result of the first drain tick.
        CountDownLatch blocker1Started = new CountDownLatch(1);
        CountDownLatch unblock1 = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blocker1Started.countDown();
            unblock1.await();
            return null;
        });
        Assert.assertTrue(blocker1Started.await(1, TimeUnit.SECONDS));

        CountDownLatch blocker2Started = new CountDownLatch(1);
        CountDownLatch unblock2 = new CountDownLatch(1);

        EmbeddedChannel ch1 = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        EmbeddedChannel ch2 = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            // Enqueue commands while the executor is blocked.
            ch1.writeInbound(Unpooled.wrappedBuffer(ping));
            ch1.writeInbound(Unpooled.wrappedBuffer(ping));
            ch2.writeInbound(Unpooled.wrappedBuffer(ping));

            // Queue a second blocker behind the first drain tick.
            eventExecutor.submit(() -> {
                blocker2Started.countDown();
                unblock2.await();
                return null;
            });

            // Allow the first drain tick to run, then block again.
            unblock1.countDown();
            Assert.assertTrue(blocker2Started.await(1, TimeUnit.SECONDS));

            // With maxDrainCommands=2 and fair scheduling, both channels should have a reply after the first tick.
            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(ch1, 1000));
            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(ch2, 1000));

            Assert.assertNull("second reply for ch1 must wait for next tick", readOutbound(ch1));
            Assert.assertNull("ch2 should have only one reply", readOutbound(ch2));

            // Allow the next drain tick to run and produce the second reply for ch1.
            unblock2.countDown();
            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(ch1, 1000));
        } finally {
            unblock1.countDown();
            unblock2.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch1.finishAndReleaseAll();
            ch2.finishAndReleaseAll();
        }
    }

    @Test
    public void frameCompactionReleasesUnderlyingBufferWhileQueued() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                processor,
                eventExecutor,
                1024,
                0,
                256,
                128,
                0,
                0,
                128,
                1000,
                NettyCommandExecutor.SchedulingPolicy.FAIR,
                1,
                1.01,
                1024 * 1024
        );
        executor.start();

        // Block the executor so the command stays queued (not executed).
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        ContextCatcher catcher = new ContextCatcher();
        EmbeddedChannel ch = new EmbeddedChannel(catcher);
        try {
            ChannelHandlerContext ctx = catcher.ctx;
            Assert.assertNotNull(ctx);

            byte[] req = ascii("*1\r\n$4\r\nPING\r\n");
            ByteBuf root = Unpooled.buffer(1024);
            try {
                root.writeBytes(req);
                ByteBuf slice = root.retainedSlice(0, req.length);

                RespCommand cmd = RespCommandBuilder.acquire(1);
                RespCommandBuilder.setArgSlice(cmd, 0, 8, 4);
                RespCommandBuilder.setFrame(cmd, new NettyRespFrame(slice));

                int before = root.refCnt();
                Assert.assertTrue("expected a derived slice to retain the root buffer", before >= 2);

                boolean accepted = executor.trySubmit(ctx, cmd);
                Assert.assertTrue(accepted);

                // Compaction runs on submit: it should replace the frame and close the old slice, reducing root refCnt.
                Assert.assertEquals(1, root.refCnt());

                unblock.countDown();
                executor.shutdownGracefully().syncUninterruptibly();
                executor.executor().submit(db::shutdown).syncUninterruptibly();
            } finally {
                root.release();
            }
        } finally {
            unblock.countDown();
            executor.close();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    private static final class ContextCatcher extends ChannelInboundHandlerAdapter {
        private volatile ChannelHandlerContext ctx;

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }
    }

    private static byte[] awaitOutbound(EmbeddedChannel ch, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();

            byte[] out = readOutbound(ch);
            if (out != null) {
                return out;
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("timeout waiting for outbound");
            }
            Thread.sleep(5);
        }
    }

    private static byte[] readOutbound(EmbeddedChannel ch) {
        ByteBuf out = ch.readOutbound();
        if (out == null) {
            return null;
        }
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
