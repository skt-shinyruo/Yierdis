package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;
import yier.bubu.redis.storage.memory.YierdisDbHealth;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import static yier.bubu.redis.testutil.TestBytes.cmd;

public class YdReconcileCommandTest {
    private static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";
    private static final String RECONCILE_FAILED = "ERR reconciliation failed";

    @Test
    public void failedReconcileKeepsDegradedAndALaterSuccessRestoresWrites() throws Exception {
        AtomicReference<Throwable> physicalReadFailure = new AtomicReference<>();
        YierdisDb db = open(physicalReadFailure);
        try {
            FastTestClient client = new FastTestClient(TestCommandComposition.createDispatcher(db));
            Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
            degrade(db);

            physicalReadFailure.set(new NativeMemoryException("physical accounting unavailable"));
            assertError(RECONCILE_FAILED, client.execute(cmd("YDRECONCILE")));
            physicalReadFailure.set(null);
            assertError(MISCONF_DEGRADED, client.execute(cmd("SET", "k", "later")));

            Assert.assertTrue(client.execute(cmd("YDRECONCILE")) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(cmd("SET", "k", "later")) instanceof ReplySimpleString);
        } finally {
            physicalReadFailure.set(null);
            db.shutdown();
        }
    }

    @Test
    public void reconcileFollowsTheSelectedDatabase() throws Exception {
        YierdisDb first = open(new AtomicReference<>());
        YierdisDb second = open(new AtomicReference<>());
        try {
            EngineSession session = new EngineSession(0, 0);
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(new YierdisDbRouter() {
                @Override
                public DbEngine dbFor(CommandSession commandSession) {
                    return commandSession.dbIndex() == 0 ? first : second;
                }

                @Override
                public int databases() {
                    return 2;
                }
            });
            FastTestClient client = new FastTestClient(dispatcher, session);
            degrade(first);
            degrade(second);

            Assert.assertTrue(client.execute(cmd("YDRECONCILE")) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(cmd("SET", "db0", "v")) instanceof ReplySimpleString);

            Assert.assertTrue(client.execute(cmd("SELECT", "1")) instanceof ReplySimpleString);
            assertError(MISCONF_DEGRADED, client.execute(cmd("SET", "db1", "v")));
            Assert.assertTrue(client.execute(cmd("YDRECONCILE")) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(cmd("SET", "db1", "v")) instanceof ReplySimpleString);
        } finally {
            first.shutdown();
            second.shutdown();
        }
    }

    private static YierdisDb open(AtomicReference<Throwable> physicalReadFailure) {
        YierdisDb db = new YierdisDbEngineFactory(
                (name, maxSlots, owner) -> failingPhysicalRead(name, maxSlots, owner, physicalReadFailure),
                0
        ).create(new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
        db.bindToCurrentThread();
        return db;
    }

    private static void degrade(YierdisDb db) throws Exception {
        Method healthMonitor = YierdisDb.class.getDeclaredMethod("healthMonitor");
        healthMonitor.setAccessible(true);
        YierdisDbHealth health = (YierdisDbHealth) healthMonitor.invoke(db);
        health.recordInvariantFailure(new IllegalStateException("derived expire count underflow"));
    }

    private static void assertError(String expected, ReplyObject reply) {
        Assert.assertTrue("expected error, got " + reply, reply instanceof ReplyError);
        Assert.assertEquals(expected, ((ReplyError) reply).message());
    }

    private static StableMemoryBackend failingPhysicalRead(
            String name,
            int maxSlots,
            MemoryOwner owner,
            AtomicReference<Throwable> physicalReadFailure
    ) {
        StableMemoryBackend delegate = new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (proxy, method, arguments) -> {
                    Throwable injected = physicalReadFailure.get();
                    if (injected != null && "memoryUsage".equals(method.getName())) {
                        throw injected;
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }
        );
    }
}
