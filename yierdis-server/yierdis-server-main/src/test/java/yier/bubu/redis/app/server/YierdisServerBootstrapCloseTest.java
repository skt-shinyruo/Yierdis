package yier.bubu.redis.app.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class YierdisServerBootstrapCloseTest {
    @Test
    public void undrainedChildStillAllowsBudgetsAndRemainingLifecycleResourcesToClose() throws Exception {
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0",
                "--replyDrainTimeoutMillis", "10"
        }));
        ChildChannelRegistry registry = new ChildChannelRegistry();
        InboundMemoryBudget inboundBudget = new InboundMemoryBudget(8_192L);
        OutboundMemoryBudget outboundBudget = new OutboundMemoryBudget(8_192L);
        EmbeddedChannel child = new EmbeddedChannel();
        CompletableFuture<Void> lifecycle = new CompletableFuture<>();
        Assert.assertEquals(ChildChannelRegistry.AdmissionResult.ACCEPTED, registry.admit(child));
        registry.bindLifecycle(child, lifecycle);
        setField(bootstrap, "childChannelRegistry", registry);
        setField(bootstrap, "inboundMemoryBudget", inboundBudget);
        setField(bootstrap, "outboundMemoryBudget", outboundBudget);

        try {
            Assert.assertThrows(IllegalStateException.class, bootstrap::close);

            Assert.assertEquals(YierdisServerBootstrap.LifecycleState.FAILED, bootstrap.lifecycleStateForTests());
            Assert.assertTrue(inboundBudget.stats().closed());
            Assert.assertTrue(outboundBudget.stats().closed());
            Assert.assertEquals(1, registry.activeChannelCount());
        } finally {
            lifecycle.complete(null);
            child.finishAndReleaseAll();
        }
        Assert.assertTrue(registry.drainedFuture().isDone());
    }

    @Test
    public void closeDrainsAcceptedChildrenBeforeClosingTheOutboundBudget() throws Exception {
        YierdisServerBootstrap bootstrap = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--replyDrainTimeoutMillis", "2000"
        );
        try (Socket child = new Socket()) {
            child.connect(new InetSocketAddress("127.0.0.1", bootstrap.port()), 2_000);
            child.setSoTimeout(2_000);

            ChildChannelRegistry registry = bootstrap.childChannelRegistryForTests();
            OutboundMemoryBudget outboundBudget = bootstrap.outboundMemoryBudgetForTests();
            awaitChildRegistration(registry);

            bootstrap.close();

            Assert.assertTrue(registry.drainedFuture().isDone());
            Assert.assertEquals(0, registry.activeChannelCount());
            Assert.assertEquals(0L, outboundBudget.stats().reservedBytes());
            Assert.assertEquals(0L, outboundBudget.stats().allocatedBytes());
            Assert.assertEquals(0L, outboundBudget.stats().activeSlots());
            Assert.assertEquals(-1, child.getInputStream().read());
        } finally {
            bootstrap.close();
        }
    }

    @Test
    public void replyDrainTimeoutReportsLiveOwnershipThenForceCloseReleasesIt() throws Exception {
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0",
                "--replyDrainTimeoutMillis", "25"
        }));
        ChildChannelRegistry registry = new ChildChannelRegistry();
        OutboundMemoryBudget budget = new OutboundMemoryBudget(8_192L);
        ReplyEgressStats egressStats = new ReplyEgressStats();
        HoldingWriteHandler holdingWrites = new HoldingWriteHandler();
        EmbeddedChannel child = new EmbeddedChannel(holdingWrites);
        child.closeFuture().addListener(ignored -> holdingWrites.failHeldWrite());
        setField(bootstrap, "childChannelRegistry", registry);
        setField(bootstrap, "outboundMemoryBudget", budget);
        setField(bootstrap, "replyEgressStats", egressStats);
        Assert.assertTrue(registry.register(child));

        OutboundConnectionMemory connectionMemory = budget.openConnection(8_192L);
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(
                child,
                connectionMemory,
                () -> { },
                slot -> {
                    throw new IllegalStateException("test does not create a reply sink");
                },
                egressStats
        );
        NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(child, 16, 1_024L);
        connection.bindReplyGate(new NettyReplyDecodedMessageGate(
                4_096L,
                4_096L,
                connectionMemory,
                sequencer
        ));
        ReplySlot slot = sequencer.register(connectionMemory.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        slot.addChunk(Unpooled.wrappedBuffer(new byte[]{'+', 'O', 'K', '\r', '\n'}));
        slot.markReady(false);
        child.runPendingTasks();
        Assert.assertEquals(ReplySlotState.WRITING, slot.state());

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = Thread.ofPlatform().start(() -> {
            try {
                bootstrap.close();
            } catch (Throwable failure) {
                closeFailure.set(failure);
            }
        });
        while (closer.isAlive()) {
            child.runPendingTasks();
            child.runScheduledPendingTasks();
            Thread.sleep(1L);
        }
        closer.join();

        try {
            Throwable failure = closeFailure.get();
            Assert.assertTrue(failure instanceof IllegalStateException);
            Assert.assertTrue(failure.getMessage().contains("reply shutdown timed out"));
            Assert.assertEquals(1L, egressStats.snapshot().shutdownTimeouts());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(0L, budget.stats().allocatedBytes());
            Assert.assertEquals(0L, budget.stats().activeSlots());
            Assert.assertEquals(0, registry.activeChannelCount());
        } finally {
            holdingWrites.failHeldWrite();
            child.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void closeAggregatesGroupShutdownFailures() throws Exception {
        List<String> closeOrder = Collections.synchronizedList(new ArrayList<>());
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0"
        }));

        setField(bootstrap, "commandGroup", failingEventExecutorGroup("command-group", closeOrder));
        setField(bootstrap, "bossGroup", failingEventLoopGroup("boss-group", closeOrder));
        setField(bootstrap, "workerGroup", failingEventLoopGroup("worker-group", closeOrder));

        try {
            bootstrap.close();
            Assert.fail("expected close failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("command-group", e.getMessage());
            Assert.assertEquals(Arrays.asList("command-group", "boss-group", "worker-group"), closeOrder);
            Assert.assertEquals(2, e.getSuppressed().length);
            Assert.assertEquals("boss-group", e.getSuppressed()[0].getMessage());
            Assert.assertEquals("worker-group", e.getSuppressed()[1].getMessage());
        }
    }

    private static YierdisServerBootstrap newBootstrap(ServerConfig config) throws Exception {
        Constructor<YierdisServerBootstrap> ctor = YierdisServerBootstrap.class.getDeclaredConstructor(ServerConfig.class);
        ctor.setAccessible(true);
        return ctor.newInstance(config);
    }

    private static void awaitChildRegistration(ChildChannelRegistry registry) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
        while (registry.activeChannelCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        Assert.assertEquals("accepted child was not registered", 1, registry.activeChannelCount());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static io.netty.util.concurrent.EventExecutorGroup failingEventExecutorGroup(String name, List<String> closeOrder) {
        return (io.netty.util.concurrent.EventExecutorGroup) Proxy.newProxyInstance(
                YierdisServerBootstrapCloseTest.class.getClassLoader(),
                new Class[]{io.netty.util.concurrent.EventExecutorGroup.class},
                (proxy, method, args) -> handleGroupInvocation(name, closeOrder, method.getName(), method.getReturnType())
        );
    }

    private static io.netty.channel.EventLoopGroup failingEventLoopGroup(String name, List<String> closeOrder) {
        return (io.netty.channel.EventLoopGroup) Proxy.newProxyInstance(
                YierdisServerBootstrapCloseTest.class.getClassLoader(),
                new Class[]{io.netty.channel.EventLoopGroup.class},
                (proxy, method, args) -> handleGroupInvocation(name, closeOrder, method.getName(), method.getReturnType())
        );
    }

    private static Object handleGroupInvocation(String name, List<String> closeOrder, String methodName, Class<?> returnType) {
        if ("shutdownGracefully".equals(methodName)) {
            closeOrder.add(name);
            return ImmediateEventExecutor.INSTANCE.newFailedFuture(new IllegalStateException(name));
        }
        if ("terminationFuture".equals(methodName)) {
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
        }
        if ("iterator".equals(methodName)) {
            Iterator<io.netty.util.concurrent.EventExecutor> iterator = Collections.<io.netty.util.concurrent.EventExecutor>emptyList().iterator();
            return iterator;
        }
        if ("next".equals(methodName)) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if ("shutdown".equals(methodName)) {
            return null;
        }
        if ("awaitTermination".equals(methodName)) {
            return false;
        }
        throw new UnsupportedOperationException("Unexpected method: " + methodName);
    }

    private static final class HoldingWriteHandler extends ChannelOutboundHandlerAdapter {
        private final AtomicReference<Object> message = new AtomicReference<>();
        private final AtomicReference<ChannelPromise> promise = new AtomicReference<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise writePromise) {
            message.set(msg);
            promise.set(writePromise);
        }

        private void failHeldWrite() {
            Object heldMessage = message.getAndSet(null);
            if (heldMessage != null) {
                ReferenceCountUtil.release(heldMessage);
            }
            ChannelPromise heldPromise = promise.getAndSet(null);
            if (heldPromise != null) {
                heldPromise.tryFailure(new IllegalStateException("channel closed during drain"));
            }
        }
    }

}
