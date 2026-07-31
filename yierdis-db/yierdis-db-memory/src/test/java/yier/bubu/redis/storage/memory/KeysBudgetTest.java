package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisGlobMatcher;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KeysBudgetTest {
    @Test
    public void keysReturnsPartialResultsWhenTimeBudgetExceeded() {
        YierdisDb db = TestDbSupport.openWithNativeSlotCapacity(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                null,
                16_384
        );
        db.bindToCurrentThread();
        try {
            // 适当放大数据量：避免在 nanoTime 分辨率较粗时出现“1ns 预算仍然跑完”的偶发现象。
            for (int i = 0; i < 4096; i++) {
                byte[] key = ("k" + i).getBytes(StandardCharsets.US_ASCII);
                byte[] val = ("v" + i).getBytes(StandardCharsets.US_ASCII);
                db.writes().strings().setString(key, val, SetMode.NORMAL, null);
            }

            try (KeyScanWindow window = db.reads().keyspace().keys(
                    "*".getBytes(StandardCharsets.US_ASCII),
                    Integer.MAX_VALUE,
                    1L
            )) {
                Assert.assertTrue(
                        "expected KEYS to return partial results under extreme time budget",
                        window.elementCount() < 4096
                );
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keysReturnsPartialResultsWhenResultLimitExceeded() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            for (int i = 0; i < 4; i++) {
                byte[] key = ("k" + i).getBytes(StandardCharsets.US_ASCII);
                db.writes().strings().setString(key, "v".getBytes(StandardCharsets.US_ASCII), SetMode.NORMAL, null);
            }

            try (KeyScanWindow window = db.reads().keyspace().keys("*".getBytes(StandardCharsets.US_ASCII), 1, 0L)) {
                Assert.assertEquals("expected KEYS to return at most the configured maxMatches", 1, window.elementCount());
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keyWindowReplaysBoundedNativeKeysWithoutRetainingResultLists() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            NativeKeyDirectory directory = db.keyLifecycle().keyDirectory();
            for (int i = 0; i < 13; i++) {
                db.writes().strings().setString(bytes("filler-" + i), bytes("v"), SetMode.NORMAL, null);
            }
            Assert.assertTrue(directory.metrics().rehashing());

            byte[] first = bytes("match:a");
            byte[] second = bytes("match:b");
            byte[] third = bytes("match:c");
            byte[] expired = bytes("match:expired");
            for (byte[] key : List.of(first, second, third)) {
                db.writes().strings().setString(key, bytes("v"), SetMode.NORMAL, null);
            }
            db.writes().strings().setString(expired, bytes("v"), SetMode.NORMAL, ExpireOption.px(0));

            byte[] tombstone = bytes("tombstone");
            db.writes().strings().setString(tombstone, bytes("v"), SetMode.NORMAL, null);
            db.writes().keyspace().del(List.of(tombstone));

            List<String> expected = matchingDirectoryOrder(db, directory, bytes("match:*"), System.currentTimeMillis());
            Assert.assertEquals(3, expected.size());
            long generationBefore = directory.tableGeneration();
            int rehashCursorBefore = directory.metrics().rehashCursor();

            try (KeyScanWindow window = db.reads().keyspace().keys(bytes("match:*"), 16, 0L)) {
                Assert.assertEquals(3, window.elementCount());
                Assert.assertEquals(
                        expected.stream().map(String::length).toList(),
                        payloadLengths(window)
                );
                Assert.assertEquals(0L, window.nextCursor().value());
                Assert.assertEquals(generationBefore, window.tableGeneration());
                Assert.assertTrue(window.expiryEvaluationMillis() > 0L);
                Assert.assertEquals(
                        (long) directory.metrics().capacity() + directory.metrics().oldCapacity(),
                        window.inspectedSlots()
                );
                Assert.assertEquals(rehashCursorBefore, directory.metrics().rehashCursor());
                assertNoResultCollections(window);

                RecordingSink sink = new RecordingSink();
                window.emitTo(sink);
                Assert.assertEquals(expected, sink.values);
                Assert.assertNotNull("KEYS must not physically delete a logically expired key", db.keyLifecycle().keyHandle(expired));
                Assert.assertEquals(rehashCursorBefore, directory.metrics().rehashCursor());
            }

            KeyScanWindow stale = db.reads().keyspace().scan(ScanCursorV2.start(), bytes("match:*"), 16);
            try {
                while (directory.metrics().rehashing()) {
                    directory.advanceRehash(HashTableWorkBudget.of(Long.MAX_VALUE, Long.MAX_VALUE));
                }
                Assert.assertFalse(stale.current());
                RecordingSink sink = new RecordingSink();
                stale.emitTo(sink);
                Assert.assertTrue("stale windows must not emit replay data", sink.values.isEmpty());
            } finally {
                stale.close();
            }
        } finally {
            db.shutdown();
        }
    }

    private static List<String> matchingDirectoryOrder(
            YierdisDb db,
            NativeKeyDirectory directory,
            byte[] pattern,
            long nowMillis
    ) {
        List<String> values = new ArrayList<>();
        ScanCursorV2 cursor = ScanCursorV2.start();
        do {
            cursor = directory.scan(cursor, 64, (key, entry) -> {
                EntryRecord record = db.keyLifecycle().entryRecord(entry);
                if (record != null
                        && !db.keyLifecycle().isKeyExpiredForScan(key, nowMillis)
                        && YierdisGlobMatcher.matches(pattern, key)) {
                    values.add(asString(key));
                }
                return true;
            });
        } while (cursor.value() != 0L);
        return values;
    }

    private static void assertNoResultCollections(KeyScanWindow window) {
        for (Field field : window.getClass().getDeclaredFields()) {
            Class<?> type = field.getType();
            Assert.assertFalse("window must not retain List fields: " + field, List.class.isAssignableFrom(type));
            Assert.assertFalse("window must not retain Collection fields: " + field, Collection.class.isAssignableFrom(type));
            Assert.assertFalse(
                    "window must not retain byte[][] fields: " + field,
                    type.isArray() && type.getComponentType().isArray() && type.getComponentType().getComponentType() == byte.class
            );
        }
    }

    private static List<Integer> payloadLengths(KeyScanWindow window) {
        List<Integer> lengths = new ArrayList<>();
        window.visitElementLengths(lengths::add);
        return lengths;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static String asString(yier.bubu.redis.storage.memory.internal.key.KeyHandle key) {
        byte[] bytes = new byte[key.length()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = key.getByte(i);
        }
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static final class RecordingSink implements ByteValueSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void value(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.US_ASCII));
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            values.add(null);
        }
    }
}
