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
import yier.bubu.redis.protocol.RespCommandDecoder;

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
                256,
                128,
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

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor, executor));
        try {
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            Assert.assertNull("first command should be queued (no reply yet)", ch.readOutbound());

            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            Assert.assertArrayEquals(ascii("-ERR busy\r\n"), readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.close();
            group.shutdownGracefully().syncUninterruptibly();
            db.shutdown();
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
                256,
                128,
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

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor, executor));
        try {
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            // Enqueue 2 commands while executor is blocked.
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            ch.writeInbound(Unpooled.wrappedBuffer(ping));

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
            Assert.assertNull("second reply must wait for next drain tick", readOutbound(ch));

            // Allow the second drain tick to run and produce the second reply.
            unblock2.countDown();
            byte[] r2 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("+PONG\r\n"), r2);
        } finally {
            unblock2.countDown();
            executor.close();
            group.shutdownGracefully().syncUninterruptibly();
            db.shutdown();
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

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor, executor));
        try {
            Assert.assertTrue(ch.config().isAutoRead());

            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");
            ch.writeInbound(Unpooled.wrappedBuffer(ping));

            ch.runPendingTasks();
            Assert.assertFalse("autoRead should be disabled after reaching high watermark", ch.config().isAutoRead());

            unblock.countDown();

            // Wait for a reply and allow scheduled tasks (flush + autoRead re-enable) to run.
            byte[] reply = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("+PONG\r\n"), reply);

            ch.runPendingTasks();
            Assert.assertTrue("autoRead should be re-enabled after backlog drains", ch.config().isAutoRead());
        } finally {
            unblock.countDown();
            executor.close();
            group.shutdownGracefully().syncUninterruptibly();
            db.shutdown();
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
