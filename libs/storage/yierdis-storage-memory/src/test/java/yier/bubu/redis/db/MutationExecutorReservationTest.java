package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.YierdisCommandException;

import java.nio.charset.StandardCharsets;

public class MutationExecutorReservationTest {
    @Test
    public void failedMutationRollsBackReservationAndDoesNotPoisonNextMutation() {
        YierdisDb db = new YierdisDb(null, 1024, "noeviction", 5, 5, 5);
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(db);

            try {
                executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
                    @Override
                    public long upperBoundBytes() {
                        return 64;
                    }

                    @Override
                    public YierdisDbMutationExecutor.MutationResult<Void> apply() {
                        throw new IllegalStateException("boom");
                    }
                });
                Assert.fail("expected mutation failure");
            } catch (IllegalStateException expected) {
                Assert.assertEquals("boom", expected.getMessage());
            }

            Assert.assertEquals(0L, db.memory().memoryStats().reservedBytes());

            byte[] key = bytes("next");
            byte[] value = bytes("ok");
            Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(value, db.reads().strings().getStringBytes(key));
            Assert.assertEquals(0L, db.memory().memoryStats().reservedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void noevictionRejectsBeforeMutationCanRun() {
        YierdisDb db = new YierdisDb(null, 1, "noeviction", 5, 5, 5);
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(db);
            boolean[] mutated = new boolean[]{false};

            try {
                executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
                    @Override
                    public long upperBoundBytes() {
                        return 64;
                    }

                    @Override
                    public YierdisDbMutationExecutor.MutationResult<Void> apply() {
                        mutated[0] = true;
                        return YierdisDbMutationExecutor.MutationResult.of(null, 64);
                    }
                });
                Assert.fail("expected OOM");
            } catch (YierdisCommandException expected) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
            }

            Assert.assertFalse(mutated[0]);
            Assert.assertNull(db.reads().strings().getStringBytes(bytes("oom")));
            Assert.assertEquals(0L, db.memory().memoryStats().reservedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void appendAndSetbitNoopsDoNotMarkValueChanged() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();
        try {
            byte[] appendKey = bytes("append");
            byte[] bitKey = bytes("bit");

            Assert.assertTrue(db.writes().strings().setString(appendKey, bytes("v"), SetMode.NORMAL, null).value());
            Assert.assertEquals(0, (int) db.writes().strings().setBit(bitKey, 0, 1).value());

            var append = db.writes().strings().append(appendKey, slice(bytes("")));
            Assert.assertEquals(1L, (long) append.value());
            Assert.assertFalse(append.mutationOutcome().valueChanged());
            Assert.assertFalse(append.changedAny());

            var setBit = db.writes().strings().setBit(bitKey, 0, 1);
            Assert.assertEquals(1, (int) setBit.value());
            Assert.assertFalse(setBit.mutationOutcome().valueChanged());
            Assert.assertFalse(setBit.changedAny());
        } finally {
            db.shutdown();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesSlice slice(byte[] data) {
        return new BytesSlice() {
            private final byte[] payload = data;

            @Override
            public int length() {
                return payload.length;
            }

            @Override
            public byte getByte(int index) {
                if (index < 0 || index >= payload.length) {
                    throw new IndexOutOfBoundsException();
                }
                return payload[index];
            }

            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(payload, 0, payload.length);
            }
        };
    }
}
