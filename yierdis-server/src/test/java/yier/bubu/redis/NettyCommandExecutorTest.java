package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NettyCommandExecutorTest {
    @Test
    public void queueFullReturnsErrBusy() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                processor,
                eventExecutor,
                1,
                0,
                256,
                128,
                0,
                0,
                128,
                10
        );
        executor.start();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        CountDownLatch blocker2Started = new CountDownLatch(1);
        CountDownLatch unblock2 = new CountDownLatch(1);

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
            Assert.assertEquals(1L, conn.commandsEnqueuedCounter().get());
            Assert.assertEquals(0L, conn.commandsExecutedCounter().get());
            Assert.assertEquals(0L, conn.commandsRejectedCounter().get());
            Assert.assertNull("first command should be queued (no reply yet)", ch.readOutbound());

            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            Assert.assertEquals(1L, conn.commandsEnqueuedCounter().get());
            Assert.assertEquals(0L, conn.commandsExecutedCounter().get());
            Assert.assertEquals(1L, conn.commandsRejectedCounter().get());
            Assert.assertArrayEquals(ascii("-ERR busy\r\n"), readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void maxDrainCommandsLimitsPerTick() throws Exception {
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
                1,
                1000
        );
        executor.start();

        // Block the executor thread so we can control when drain ticks run.
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

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            // Enqueue 2 commands while executor is blocked.
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
            Assert.assertEquals(2L, conn.commandsEnqueuedCounter().get());
            Assert.assertEquals(0L, conn.commandsExecutedCounter().get());

            // Queue a second blocker behind the first drain tick, so we can observe intermediate state.
            eventExecutor.submit(() -> {
                blocker2Started.countDown();
                unblock2.await();
                return null;
            });

            // Allow one drain tick to run (maxDrainCommands=1), then the second blocker takes over.
            unblock1.countDown();
            Assert.assertTrue(blocker2Started.await(1, TimeUnit.SECONDS));

            // Only the first command should have been processed in the first drain tick.
            byte[] r1 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("+PONG\r\n"), r1);
            Assert.assertEquals(1L, conn.commandsExecutedCounter().get());
            Assert.assertNull("second reply must wait for next drain tick", readOutbound(ch));

            // Allow the second drain tick to run and produce the second reply.
            unblock2.countDown();
            byte[] r2 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("+PONG\r\n"), r2);
            Assert.assertEquals(2L, conn.commandsExecutedCounter().get());
        } finally {
            unblock2.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void autoReadIsDisabledAndReenabledWithHysteresis() throws Exception {
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
                1,
                0,
                0,
                0,
                128,
                10
        );
        executor.start();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            Assert.assertTrue(ch.config().isAutoRead());

            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
            Assert.assertEquals(1L, conn.commandsEnqueuedCounter().get());

            ch.runPendingTasks();
            Assert.assertFalse("autoRead should be disabled after reaching high watermark", ch.config().isAutoRead());
            Assert.assertEquals(1L, conn.backpressureEnterCounter().get());
            Assert.assertEquals(0L, conn.backpressureExitCounter().get());

            unblock.countDown();

            // Wait for a reply and allow scheduled tasks (flush + autoRead re-enable) to run.
            byte[] reply = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("+PONG\r\n"), reply);

            ch.runPendingTasks();
            Assert.assertTrue("autoRead should be re-enabled after backlog drains", ch.config().isAutoRead());
            Assert.assertEquals(1L, conn.backpressureEnterCounter().get());
            Assert.assertEquals(1L, conn.backpressureExitCounter().get());
            Assert.assertEquals(1L, conn.commandsExecutedCounter().get());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void queuedBytesBudgetRejectsAndRecoversAfterDrain() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                processor,
                eventExecutor,
                16,
                7,
                256,
                128,
                0,
                0,
                128,
                10
        );
        executor.start();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            byte[] pingInline = ascii("PING\r\n");

            ch.writeInbound(Unpooled.wrappedBuffer(pingInline));
            Assert.assertNull("first command should be queued (no reply yet)", ch.readOutbound());

            NettyCommandExecutor.StatsSnapshot s1 = executor.statsSnapshot();
            Assert.assertTrue("expected queued bytes > 0 when queueMaxBytes is enabled", s1.queuedBytes > 0);

            ch.writeInbound(Unpooled.wrappedBuffer(pingInline));
            Assert.assertArrayEquals(ascii("-ERR busy\r\n"), readOutbound(ch));

            NettyCommandExecutor.StatsSnapshot s2 = executor.statsSnapshot();
            Assert.assertEquals("reject path must not leak queued bytes", s1.queuedBytes, s2.queuedBytes);

            unblock.countDown();

            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(ch, 1000));

            NettyCommandExecutor.StatsSnapshot s3 = executor.statsSnapshot();
            Assert.assertEquals("after drain, queued tasks should be released", 0, s3.queuedTasks);
            Assert.assertEquals("after drain, queued bytes should be released", 0L, s3.queuedBytes);

            ch.writeInbound(Unpooled.wrappedBuffer(pingInline));
            Assert.assertArrayEquals("budget should recover after drain", ascii("+PONG\r\n"), awaitOutbound(ch, 1000));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void autoReadIsDisabledAndReenabledWithBytesHysteresis() throws Exception {
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
                1,
                0,
                128,
                10
        );
        executor.start();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            Assert.assertTrue(ch.config().isAutoRead());

            byte[] pingInline = ascii("PING\r\n");
            ch.writeInbound(Unpooled.wrappedBuffer(pingInline));

            ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
            Assert.assertEquals(1L, conn.commandsEnqueuedCounter().get());
            Assert.assertEquals(1, conn.pendingCounter().get());
            Assert.assertTrue("sanity: pending should be below high watermark, so bytes must be the trigger", conn.pendingCounter().get() < 256);
            Assert.assertTrue("sanity: pending bytes should be > 0", conn.pendingBytesCounter().get() > 0);

            ch.runPendingTasks();
            Assert.assertFalse("autoRead should be disabled after reaching bytes high watermark", ch.config().isAutoRead());
            Assert.assertEquals(1L, conn.backpressureEnterCounter().get());
            Assert.assertEquals(0L, conn.backpressureExitCounter().get());

            unblock.countDown();

            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(ch, 1000));

            ch.runPendingTasks();
            Assert.assertTrue("autoRead should be re-enabled after bytes backlog drains", ch.config().isAutoRead());
            Assert.assertEquals(1L, conn.backpressureEnterCounter().get());
            Assert.assertEquals(1L, conn.backpressureExitCounter().get());
            Assert.assertEquals(1L, conn.commandsExecutedCounter().get());
            Assert.assertEquals(0, conn.pendingCounter().get());
            Assert.assertEquals(0L, conn.pendingBytesCounter().get());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void quitClosesConnectionAndSkipsFollowupCommands() throws Exception {
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
                10
        );
        executor.start();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        try {
            byte[] quit = ascii("*1\r\n$4\r\nQUIT\r\n");
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            // Enqueue QUIT + PING while executor is blocked, so both are accepted.
            ch.writeInbound(Unpooled.wrappedBuffer(quit));
            ch.writeInbound(Unpooled.wrappedBuffer(ping));

            ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
            Assert.assertEquals(2L, conn.commandsEnqueuedCounter().get());
            Assert.assertEquals(0L, conn.closeAfterReplyCounter().get());

            unblock.countDown();

            byte[] r1 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("+OK\r\n"), r1);

            // No reply for the followup PING; it should be skipped after closing is requested.
            Assert.assertNull(readOutbound(ch));

            // Allow close/flush and any scheduled tasks to run.
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();

            Assert.assertEquals(1L, conn.closeAfterReplyCounter().get());
            Assert.assertEquals(1L, conn.commandsExecutedCounter().get());
            Assert.assertEquals(1L, conn.commandsSkippedClosingCounter().get());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(db::shutdown).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
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
