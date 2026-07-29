package yier.bubu.redis.integration.runtime;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.integration.command.TestCommandDispatchers;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.embedded.YierdisInstanceMaintenance;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;
import yier.bubu.redis.testutil.TestYierdisInstances;

import static yier.bubu.redis.testutil.TestBytes.b;

public class CommitStreamExpirationEvictionTest {
    @Test
    public void fullCommitStreamLeavesExpiredKeyLogicallyInvisibleUntilActiveCleanupCanPublish() throws Exception {
        List<DeliveredEvent> events = new ArrayList<>();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        AtomicBoolean blockSink = new AtomicBoolean();
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .changeSink(event -> {
                    synchronized (events) {
                        events.add(new DeliveredEvent(
                                event.sequence(),
                                event.kind(),
                                event.synthetic(),
                                event.request().argc(),
                                ascii(event.request().toByteArray(0)),
                                ascii(event.request().toByteArray(1))
                        ));
                    }
                    if (blockSink.get()) {
                        callbackEntered.countDown();
                        try {
                            Assert.assertTrue("timed out waiting to release commit-stream sink",
                                    releaseCallback.await(5L, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                })
                .commitStreamMaxEvents(1)
                .commitStreamMaxRetainedBytes(1_024L * 1_024L)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandDispatchers.forRouter(TestDbRouters.forInstance(instance));
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertEquals("OK", ((ReplySimpleString) executeScoped(
                        client, List.of(b("SET"), b("expiring"), b("value"))
                )).value());
                awaitEventCount(events, 1);
                awaitReservedEvents(instance, 0L);

                Assert.assertEquals(1L, ((yier.bubu.redis.testutil.ReplyInteger) executeScoped(
                        client, List.of(b("PEXPIRE"), b("expiring"), b("1"))
                )).value());
                awaitEventCount(events, 2);
                awaitReservedEvents(instance, 0L);

                blockSink.set(true);
                Assert.assertEquals("OK", ((ReplySimpleString) executeScoped(
                        client, List.of(b("SET"), b("blocker"), b("value"))
                )).value());
                Assert.assertTrue("blocking event did not enter the sink", callbackEntered.await(5L, TimeUnit.SECONDS));

                Thread.sleep(20L);
                Assert.assertSame(ReplyNull.INSTANCE, client.execute(List.of(b("GET"), b("expiring"))));
                Assert.assertEquals("stream pressure must defer physical expiration", 2, physicalKeyCount(instance));
                Assert.assertEquals(1L,
                        instance.observability().memoryStats().expiredEntriesAwaitingPhysicalDeletion());
                synchronized (events) {
                    Assert.assertEquals(3, events.size());
                }

                blockSink.set(false);
                releaseCallback.countDown();
                awaitReservedEvents(instance, 0L);
                new YierdisInstanceMaintenance(instance).maintenanceTick();
                awaitEventCount(events, 4);
                Assert.assertEquals(1, physicalKeyCount(instance));
                Assert.assertEquals(0L,
                        instance.observability().memoryStats().expiredEntriesAwaitingPhysicalDeletion());

                synchronized (events) {
                    DeliveredEvent expiry = events.get(3);
                    Assert.assertEquals(YierdisChangeKind.EXPIRED, expiry.kind());
                    Assert.assertTrue(expiry.synthetic());
                    Assert.assertEquals(2, expiry.argc());
                    Assert.assertEquals("DEL", expiry.command());
                    Assert.assertEquals("expiring", expiry.key());
                    Assert.assertEquals(4L, expiry.sequence());
                }
            } finally {
                blockSink.set(false);
                releaseCallback.countDown();
            }
        }
    }

    @Test
    public void fullGlobalCommitStreamRejectsEvictionWithBusyAndKeepsVictim() throws Exception {
        byte[] value = new byte[64 * 1024];
        Arrays.fill(value, (byte) 'v');
        long maxmemoryBytes = minGlobalMaxmemoryThatAllowsKeyCount(value, 2) - 1L;

        List<DeliveredEvent> events = new ArrayList<>();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        AtomicBoolean blockSink = new AtomicBoolean(true);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
                .maxmemorySamples(4)
                .evictionTimeLimitMillis(1_000L)
                .changeSink(event -> {
                    synchronized (events) {
                        events.add(new DeliveredEvent(
                                event.sequence(),
                                event.kind(),
                                event.synthetic(),
                                event.request().argc(),
                                ascii(event.request().toByteArray(0)),
                                ascii(event.request().toByteArray(1))
                        ));
                    }
                    if (blockSink.get()) {
                        callbackEntered.countDown();
                        try {
                            Assert.assertTrue("timed out waiting to release commit-stream sink",
                                    releaseCallback.await(5L, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                })
                .commitStreamMaxEvents(1)
                .commitStreamMaxRetainedBytes(1_024L * 1_024L)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandDispatchers.forRouter(TestDbRouters.forInstance(instance));
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                Assert.assertEquals("OK", ((ReplySimpleString) executeScoped(
                        client, List.of(b("SET"), b("victim"), value)
                )).value());
                Assert.assertTrue("first commit event did not enter the blocking sink",
                        callbackEntered.await(5L, TimeUnit.SECONDS));

                ReplyObject rejected = executeScoped(client, List.of(b("SET"), b("incoming"), value));

                Assert.assertTrue(rejected instanceof ReplyError);
                Assert.assertEquals("BUSY commit stream unavailable", ((ReplyError) rejected).message());
                Assert.assertArrayEquals(value,
                        ((ReplyBulkString) client.execute(List.of(b("GET"), b("victim")))).data());
                Assert.assertSame(ReplyNull.INSTANCE, client.execute(List.of(b("GET"), b("incoming"))));

                blockSink.set(false);
                releaseCallback.countDown();
                awaitReservedEvents(instance, 0L);

                Assert.assertEquals("OK", ((ReplySimpleString) executeScoped(
                        client, List.of(b("SET"), b("incoming"), value)
                )).value());
                Assert.assertSame(ReplyNull.INSTANCE, client.execute(List.of(b("GET"), b("victim"))));
                Assert.assertArrayEquals(value,
                        ((ReplyBulkString) client.execute(List.of(b("GET"), b("incoming")))).data());
                awaitEventCount(events, 3);

                synchronized (events) {
                    Assert.assertEquals(3, events.size());
                    assertUserSet(events.get(0), "victim");
                    DeliveredEvent eviction = events.get(1);
                    Assert.assertEquals(YierdisChangeKind.EVICTED, eviction.kind());
                    Assert.assertTrue(eviction.synthetic());
                    Assert.assertEquals(2, eviction.argc());
                    Assert.assertEquals("DEL", eviction.command());
                    Assert.assertEquals("victim", eviction.key());
                    assertUserSet(events.get(2), "incoming");
                    Assert.assertEquals(1L, events.get(0).sequence());
                    Assert.assertEquals(2L, eviction.sequence());
                    Assert.assertEquals(3L, events.get(2).sequence());
                }
            } finally {
                blockSink.set(false);
                releaseCallback.countDown();
            }
        }
    }

    private static void assertUserSet(DeliveredEvent event, String key) {
        Assert.assertEquals(YierdisChangeKind.USER_COMMAND, event.kind());
        Assert.assertFalse(event.synthetic());
        Assert.assertEquals(3, event.argc());
        Assert.assertEquals("SET", event.command());
        Assert.assertEquals(key, event.key());
    }

    private static int physicalKeyCount(YierdisInstance instance) {
        return instance.observability().dbSummaries().get(0).keyCount();
    }

    private static ReplyObject executeScoped(FastTestClient client, List<byte[]> command) {
        return client.execute(ByteArrayExecutionRequest.copyOf(command));
    }

    private static void awaitReservedEvents(YierdisInstance instance, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (instance.observability().commitStreamStats().reservedEvents() != expected) {
            if (System.nanoTime() >= deadline) {
                Assert.fail("commit stream did not reach reserved event count " + expected);
            }
            Thread.sleep(1L);
        }
    }

    private static void awaitEventCount(List<DeliveredEvent> events, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (true) {
            synchronized (events) {
                if (events.size() == expected) {
                    return;
                }
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("commit stream did not deliver " + expected + " events");
            }
            Thread.sleep(1L);
        }
    }

    private static long minGlobalMaxmemoryThatAllowsKeyCount(byte[] value, int count) {
        long high = 1L;
        while (!allowsGlobalKeyCount(high, value, count)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (allowsGlobalKeyCount(mid, value, count)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static boolean allowsGlobalKeyCount(long maxmemoryBytes, byte[] value, int count) {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            for (int i = 0; i < count; i++) {
                instance.engine(0).writes().strings().setString(b("probe-" + i), value, SetMode.NORMAL, null);
            }
            return true;
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        }
    }

    private static String ascii(byte[] value) {
        return new String(value, StandardCharsets.US_ASCII);
    }

    private record DeliveredEvent(
            long sequence,
            YierdisChangeKind kind,
            boolean synthetic,
            int argc,
            String command,
            String key
    ) {
    }
}
