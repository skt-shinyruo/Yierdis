package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.testkit.MaterializingByteValueSink;
import yier.bubu.redis.storage.testkit.TestBytes;

public class PreparedMutationStorageTest {
    @Test
    public void popPreviewAndValidationDoNotMutateAndStalePreparationCannotCommit() {
        try (TestDb db = heapDb()) {
            db.push("list", List.of("a", "b", "c"));
            try (PreparedMutation<PoppedValueSequence> prepared =
                         db.lists().preparePop(bytes("list"), 2, true)) {
                Assert.assertEquals(List.of("a", "b"), strings(prepared.preview()));
                Assert.assertTrue(prepared.isCurrent());
                Assert.assertEquals(List.of("a", "b", "c"), db.list("list"));

                db.lists().rpush(bytes("list"), List.of(bytes("d")));
                Assert.assertFalse(prepared.isCurrent());
                Assert.assertThrows(
                        IllegalStateException.class,
                        prepared::commit
                );
            }
            Assert.assertEquals(List.of("a", "b", "c", "d"), db.list("list"));
        }
    }

    @Test
    public void currentPopCommitsOnceAndKeepsPreviewReadableUntilClose() {
        try (TestDb db = heapDb()) {
            db.push("list", List.of("a", "b", "c"));
            try (PreparedMutation<PoppedValueSequence> prepared =
                         db.lists().preparePop(bytes("list"), 2, true)) {
                PoppedValueSequence preview = prepared.preview();
                MutationOutcome outcome = prepared.commit();

                Assert.assertTrue(outcome.changedAny());
                Assert.assertEquals(List.of("a", "b"), strings(preview));
                Assert.assertEquals(List.of("c"), db.list("list"));
                Assert.assertThrows(
                        IllegalStateException.class,
                        prepared::commit
                );
            }
        }
    }

    @Test
    public void setPreviewAndValidationDoNotMutateAndFreshPreparationCommitsOnce() {
        try (TestDb db = heapDb()) {
            db.set("key", "old");
            try (PreparedMutation<StringOps.SetStringValue> stale =
                         db.stringOps().prepareSet(
                                 bytes("key"), slice("new"), SetMode.NORMAL, null, true
                         )) {
                Assert.assertTrue(stale.preview().applied());
                Assert.assertEquals("old", string(stale.preview().oldValue()));
                Assert.assertTrue(stale.isCurrent());
                Assert.assertEquals("old", db.string("key"));

                db.set("key", "intervening");
                Assert.assertFalse(stale.isCurrent());
                Assert.assertThrows(
                        IllegalStateException.class,
                        stale::commit
                );
            }
            Assert.assertEquals("intervening", db.string("key"));

            try (PreparedMutation<StringOps.SetStringValue> fresh =
                         db.stringOps().prepareSet(
                                 bytes("key"), slice("fresh"), SetMode.NORMAL, null, true
                         )) {
                StringOps.SetStringValue preview = fresh.preview();
                Assert.assertTrue(preview.applied());
                Assert.assertEquals("intervening", string(preview.oldValue()));
                Assert.assertEquals("intervening", db.string("key"));

                MutationOutcome outcome = fresh.commit();

                Assert.assertTrue(outcome.changedAny());
                Assert.assertEquals("intervening", string(preview.oldValue()));
                Assert.assertEquals("fresh", db.string("key"));
                Assert.assertThrows(
                        IllegalStateException.class,
                        fresh::commit
                );
            }
        }
    }

    @Test
    public void setWithoutGetDoesNotRetainTheSupersededStringForItsPreview() {
        try (TestDb db = heapDb()) {
            db.set("key", "old");

            try (PreparedMutation<StringOps.SetStringValue> prepared =
                         db.stringOps().prepareSet(
                                 bytes("key"), slice("new"), SetMode.NORMAL, null, false
                         )) {
                Assert.assertTrue(prepared.preview().applied());
                Assert.assertTrue(prepared.preview().oldValue().payloadLength() < 0);
                Assert.assertEquals(0L, prepared.preview().oldValue().retainedMemoryBytes());
            }
        }
    }

    private static TestDb heapDb() {
        RuntimeDbEngine engine = new YierdisDbEngineFactory(
                HeapStableMemoryBackend::new,
                4096
        ).create(new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
        engine.bindToCurrentThread();
        return new TestDb(engine);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static BytesSlice slice(String value) {
        return TestBytes.slice(bytes(value));
    }

    private static List<String> strings(ByteSequenceSource source) {
        CollectingSink sink = new CollectingSink();
        source.emitTo(sink);
        return sink.values();
    }

    private static String string(ByteValue value) {
        CollectingSink sink = new CollectingSink();
        value.emitTo(sink);
        Assert.assertEquals(1, sink.values().size());
        return sink.values().get(0);
    }

    private static final class TestDb implements AutoCloseable {
        private final RuntimeDbEngine engine;

        private TestDb(RuntimeDbEngine engine) {
            this.engine = engine;
        }

        private yier.bubu.redis.storage.api.ListOps lists() {
            return engine.lists();
        }

        private StringOps stringOps() {
            return engine.strings();
        }

        private void push(String key, List<String> values) {
            engine.lists().rpush(
                    bytes(key),
                    values.stream().map(PreparedMutationStorageTest::bytes).toList()
            );
        }

        private List<String> list(String key) {
            try (ByteSequenceSource source = engine.lists().lrange(bytes(key), 0, -1)) {
                return strings(source);
            }
        }

        private void set(String key, String value) {
            engine.strings().setString(bytes(key), bytes(value), SetMode.NORMAL, null);
        }

        private String string(String key) {
            byte[] value = OwnedReplyValueAssertions.stringValue(engine.strings(), bytes(key));
            return value == null ? null : new String(value, StandardCharsets.US_ASCII);
        }

        @Override
        public void close() {
            engine.shutdown();
        }
    }

    private static final class CollectingSink extends MaterializingByteValueSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        private List<String> values() {
            return new ArrayList<>(values);
        }
    }
}
