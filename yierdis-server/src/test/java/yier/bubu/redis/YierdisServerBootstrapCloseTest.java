package yier.bubu.redis;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBuf;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.EvictionCoordinator;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.KeyspaceOps;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.RuntimeDbEngine;
import yier.bubu.redis.ops.TtlOps;
import yier.bubu.redis.ops.ValueOps;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class YierdisServerBootstrapCloseTest {
    @Test
    public void closeAggregatesGroupShutdownFailuresAndStillClosesAllocator() throws Exception {
        List<String> closeOrder = Collections.synchronizedList(new ArrayList<>());
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[0]));

        setField(bootstrap, "commandGroup", failingEventExecutorGroup("command-group", closeOrder));
        setField(bootstrap, "bossGroup", failingEventLoopGroup("boss-group", closeOrder));
        setField(bootstrap, "workerGroup", failingEventLoopGroup("worker-group", closeOrder));
        setField(bootstrap, "offHeapAllocator", new ThrowingAllocator(closeOrder));

        try {
            bootstrap.close();
            Assert.fail("expected close failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("command-group", e.getMessage());
            Assert.assertEquals(Arrays.asList("command-group", "boss-group", "worker-group", "allocator"), closeOrder);
            Assert.assertEquals(3, e.getSuppressed().length);
            Assert.assertEquals("boss-group", e.getSuppressed()[0].getMessage());
            Assert.assertEquals("worker-group", e.getSuppressed()[1].getMessage());
            Assert.assertEquals("allocator", e.getSuppressed()[2].getMessage());
        }
    }

    @Test
    public void closePropagatesInstanceAndAllocatorFailuresAfterBestEffortCleanup() throws Exception {
        List<String> closeOrder = Collections.synchronizedList(new ArrayList<>());
        DefaultEventExecutorGroup commandGroup = new DefaultEventExecutorGroup(1);
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[0]));

        YierdisInstance instance = null;
        try {
            DbEngineFactory factory = (dbIndex,
                                       offHeapAllocator,
                                       ownsOffHeapAllocator,
                                       offHeapKeysEnabled,
                                       maxmemoryBytes,
                                       maxmemoryPolicy,
                                       maxmemorySamples,
                                       evictionTimeLimitMillis,
                                       expireCleanupTimeLimitMillis) -> new FailingRuntimeDbEngine("db-" + dbIndex, closeOrder);
            instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                    .databases(1)
                    .engineFactory(factory)
                    .ownsOffHeapAllocator(false)
                    .build());

            YierdisFastCommandProcessor processor = instance.newCommandProcessor();
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    commandGroup.next(),
                    new yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory(),
                    16,
                    0,
                    256,
                    128,
                    0,
                    0,
                    128,
                    10,
                    SchedulingPolicy.FAIR
            );
            executor.start();

            setField(bootstrap, "instance", instance);
            setField(bootstrap, "executor", executor);
            setField(bootstrap, "commandGroup", commandGroup);
            setField(bootstrap, "offHeapAllocator", new ThrowingAllocator(closeOrder));

            try {
                bootstrap.close();
                Assert.fail("expected close failure");
            } catch (IllegalStateException e) {
                Assert.assertEquals("db-0", e.getMessage());
                Assert.assertEquals(Arrays.asList("db-0", "allocator"), closeOrder);
                Assert.assertEquals(1, e.getSuppressed().length);
                Assert.assertEquals("allocator", e.getSuppressed()[0].getMessage());
            }
            instance = null;
        } finally {
            if (instance != null) {
                try {
                    instance.close();
                } catch (Throwable ignored) {
                }
            }
            if (!commandGroup.isShuttingDown()) {
                commandGroup.shutdownGracefully().syncUninterruptibly();
            }
        }
    }

    private static YierdisServerBootstrap newBootstrap(ServerConfig config) throws Exception {
        Constructor<YierdisServerBootstrap> ctor = YierdisServerBootstrap.class.getDeclaredConstructor(ServerConfig.class);
        ctor.setAccessible(true);
        return ctor.newInstance(config);
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

    private static final class FailingRuntimeDbEngine implements RuntimeDbEngine {
        private final String name;
        private final List<String> closeOrder;

        private FailingRuntimeDbEngine(String name, List<String> closeOrder) {
            this.name = name;
            this.closeOrder = closeOrder;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void shutdown() {
            closeOrder.add(name);
            throw new IllegalStateException(name);
        }

        @Override
        public ValueOps values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EvictionCoordinator eviction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyspaceOps keyspace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TtlOps ttl() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ThrowingAllocator implements YierdisOffHeapAllocator {
        private final List<String> closeOrder;

        private ThrowingAllocator(List<String> closeOrder) {
            this.closeOrder = closeOrder;
        }

        @Override
        public YierdisOffHeapBuf allocate(int capacity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long usedBytes() {
            return 0;
        }

        @Override
        public long maxBytes() {
            return 0;
        }

        @Override
        public YierdisOffHeapBackend backend() {
            return YierdisOffHeapBackend.NONE;
        }

        @Override
        public void close() {
            closeOrder.add("allocator");
            throw new IllegalStateException("allocator");
        }
    }
}
