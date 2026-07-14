package yier.bubu.redis.integration.runtime;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.command.CommandRecordScope;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.testutil.TestYierdisInstances;

public class CommitStreamIntegrationTest {
    @Test
    public void scopedChangedWritePublishesOnceAndNoopConsumesNoEvent() throws Exception {
        List<DeliveredEvent> events = new ArrayList<>();
        CountDownLatch changed = new CountDownLatch(1);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .changeSink(event -> {
                    synchronized (events) {
                        events.add(new DeliveredEvent(
                                event.sequence(),
                                event.dbIndex(),
                                ascii(event.request().toByteArray(0)),
                                ascii(event.request().toByteArray(1)),
                                event.synthetic(),
                                event.kind()
                        ));
                    }
                    changed.countDown();
                })
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            try (ExecutionRequest record = ByteArrayExecutionRequest.fromUtf8("SET", List.of("key", "value"));
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertTrue(instance.engine(0).writes().strings()
                        .setString(bytes("key"), bytes("value"), SetMode.NORMAL, null).value());
            }

            Assert.assertTrue("commit stream did not deliver changed write", changed.await(5, TimeUnit.SECONDS));
            synchronized (events) {
                Assert.assertEquals(1, events.size());
                Assert.assertEquals(1L, events.get(0).sequence());
                Assert.assertEquals(0, events.get(0).dbIndex());
                Assert.assertEquals("SET", events.get(0).command());
                Assert.assertEquals("key", events.get(0).key());
                Assert.assertFalse(events.get(0).synthetic());
                events.clear();
            }

            try (ExecutionRequest record = ByteArrayExecutionRequest.fromUtf8("SET", List.of("key", "other", "NX"));
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertFalse(instance.engine(0).writes().strings()
                        .setString(bytes("key"), bytes("other"), SetMode.NX, null).value());
            }

            Thread.sleep(50L);
            synchronized (events) {
                Assert.assertTrue(events.isEmpty());
            }
        }
    }

    @Test
    public void passiveExpiryPublishesAnExpiredDeleteThroughTheCommitStream() throws Exception {
        List<DeliveredEvent> events = new ArrayList<>();
        CountDownLatch delivered = new CountDownLatch(3);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .changeSink(event -> {
                    synchronized (events) {
                        events.add(new DeliveredEvent(
                                event.sequence(),
                                event.dbIndex(),
                                ascii(event.request().toByteArray(0)),
                                ascii(event.request().toByteArray(1)),
                                event.synthetic(),
                                event.kind()
                        ));
                    }
                    delivered.countDown();
                })
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            try (ExecutionRequest record = ByteArrayExecutionRequest.fromUtf8("SET", List.of("expiring", "value"));
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertTrue(instance.engine(0).writes().strings()
                        .setString(bytes("expiring"), bytes("value"), SetMode.NORMAL, null).value());
            }
            try (ExecutionRequest record = ByteArrayExecutionRequest.fromUtf8("PEXPIRE", List.of("expiring", "1"));
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertTrue(instance.engine(0).writes().ttl()
                        .pexpire(view(bytes("expiring")), 1L).value());
            }

            Thread.sleep(20L);
            Assert.assertNull(instance.engine(0).reads().strings().getStringBytes(bytes("expiring")));
            Assert.assertTrue("commit stream did not deliver expiry", delivered.await(5, TimeUnit.SECONDS));

            synchronized (events) {
                Assert.assertEquals(3, events.size());
                DeliveredEvent expiry = events.get(2);
                Assert.assertEquals("DEL", expiry.command());
                Assert.assertEquals("expiring", expiry.key());
                Assert.assertTrue(expiry.synthetic());
                Assert.assertEquals(YierdisChangeKind.EXPIRED, expiry.kind());
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static String ascii(byte[] value) {
        return new String(value, StandardCharsets.US_ASCII);
    }

    private static yier.bubu.redis.bytes.BytesView view(byte[] bytes) {
        return new yier.bubu.redis.bytes.BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }

    private record DeliveredEvent(
            long sequence,
            int dbIndex,
            String command,
            String key,
            boolean synthetic,
            YierdisChangeKind kind
    ) {
    }
}
