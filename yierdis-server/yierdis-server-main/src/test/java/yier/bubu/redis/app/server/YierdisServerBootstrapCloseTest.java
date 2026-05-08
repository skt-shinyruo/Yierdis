package yier.bubu.redis.app.server;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

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
    public void closeAggregatesGroupShutdownFailures() throws Exception {
        List<String> closeOrder = Collections.synchronizedList(new ArrayList<>());
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[0]));

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

    @Test
    public void closeWithoutExecutorStillPropagatesInstanceFailures() throws Exception {
        List<String> closeOrder = Collections.synchronizedList(new ArrayList<>());
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[0]));

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactory((dbIndex,
                                maxmemoryBytes,
                                maxmemoryPolicy,
                                maxmemorySamples,
                                evictionTimeLimitMillis,
                                expireCleanupTimeLimitMillis) -> new FailingRuntimeDbEngine("db-" + dbIndex, closeOrder))
                .build());

        setField(bootstrap, "instance", instance);

        try {
            bootstrap.close();
            Assert.fail("expected close failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("db-0", e.getMessage());
            Assert.assertEquals(Arrays.asList("db-0"), closeOrder);
            Assert.assertEquals(0, e.getSuppressed().length);
        }
    }

    @Test
    public void closePropagatesInstanceFailuresAfterBestEffortCleanup() throws Exception {
        List<String> closeOrder = Collections.synchronizedList(new ArrayList<>());
        DefaultEventExecutorGroup commandGroup = new DefaultEventExecutorGroup(1);
        YierdisServerBootstrap bootstrap = newBootstrap(ServerConfig.fromArgs(new String[0]));

        YierdisInstance instance = null;
        try {
            DbEngineFactory factory = (dbIndex,
                                       maxmemoryBytes,
                                       maxmemoryPolicy,
                                       maxmemorySamples,
                                       evictionTimeLimitMillis,
                                       expireCleanupTimeLimitMillis) -> new FailingRuntimeDbEngine("db-" + dbIndex, closeOrder);
            instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                    .databases(1)
                    .engineFactory(factory)
                    .build());

            YierdisEngine engine = TestYierdisEngines.forInstance(instance);
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    commandGroup.next(),
                    new yier.bubu.redis.protocol.custom.v1.execution.JsonLineReplyWriterFactory(),
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();

            setField(bootstrap, "instance", instance);
            setField(bootstrap, "executor", executor);
            setField(bootstrap, "commandGroup", commandGroup);

            try {
                bootstrap.close();
                Assert.fail("expected close failure");
            } catch (IllegalStateException e) {
                Assert.assertEquals("db-0", e.getMessage());
                Assert.assertEquals(Arrays.asList("db-0"), closeOrder);
                Assert.assertEquals(0, e.getSuppressed().length);
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
        public void enforceMaxmemoryMaintenance() {
        }

        @Override
        public void shutdown() {
            closeOrder.add(name);
            throw new IllegalStateException(name);
        }

        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
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

}
