package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

import java.nio.charset.StandardCharsets;

public class MutationExecutorReservationTest {
    private static final long PREPARED_TEST_UPPER_BOUND_BYTES = 1_000_000L;

    private InMemoryLedger preparedLedger;
    private YierdisFfmMemoryRuntime preparedRuntime;
    private YierdisStableNativeAllocator preparedAllocator;
    private YierdisDbMutationExecutor preparedExecutor;

    @Before
    public void setUpPreparedFixture() {
        preparedLedger = new InMemoryLedger(0);
        preparedRuntime = new YierdisFfmMemoryRuntime("prepared-mutation-test");
        preparedAllocator = new YierdisStableNativeAllocator(preparedRuntime, 128);
        preparedAllocator.bindToCurrentThread();
        preparedExecutor = new YierdisDbMutationExecutor(
                () -> {
                },
                preparedLedger,
                preparedAllocator
        );
    }

    @After
    public void tearDownPreparedFixture() {
        if (preparedAllocator != null) {
            preparedAllocator.close();
        }
        if (preparedRuntime != null) {
            preparedRuntime.close();
        }
    }

    @Test
    public void preparationFailureAbortsAndRollsBackReservation() {
        AtomicBoolean aborted = new AtomicBoolean();
        PreparedDbMutation<String> prepared = prepared(
                7,
                0,
                MutationOutcome.VALUE_CHANGED,
                () -> "ok",
                () -> {
                },
                () -> aborted.set(true)
        );

        YierdisCommandException failure = org.junit.Assert.assertThrows(
                YierdisCommandException.class,
                () -> preparedExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<String>() {
                    @Override
                    public long upperBoundBytes() {
                        return PREPARED_TEST_UPPER_BOUND_BYTES;
                    }

                    @Override
                    public PreparedDbMutation<String> prepare() {
                        prepared.abort();
                        throw new NativeCapacityExceededException("injected");
                    }
                })
        );
        org.junit.Assert.assertEquals(MaxmemoryErrors.OOM_ERR, failure.getMessage());
        org.junit.Assert.assertTrue(aborted.get());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    @Test
    public void commitRunsAfterReconcileAndCannotBeRepeated() {
        List<String> order = new ArrayList<>();
        PreparedDbMutation<String> prepared = prepared(
                7,
                0,
                MutationOutcome.VALUE_CHANGED,
                () -> {
                    order.add("commit");
                    return "ok";
                },
                () -> order.add("release"),
                () -> order.add("abort")
        );

        org.junit.Assert.assertEquals(
                "ok",
                preparedExecutor.execute(plan(PREPARED_TEST_UPPER_BOUND_BYTES, prepared))
        );
        org.junit.Assert.assertEquals(List.of("commit", "release"), order);
        org.junit.Assert.assertEquals(7L, preparedLedger.usedBytes());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
        org.junit.Assert.assertThrows(IllegalStateException.class, prepared::commit);
        prepared.releaseSuperseded();
        org.junit.Assert.assertEquals(List.of("commit", "release"), order);
    }

    @Test
    public void reclamationAdmissionDoesNotReenterCleanupOrGovernor() {
        preparedLedger.commit(null, 7);
        PreparedDbMutation<String> prepared = prepared(
                -7,
                0,
                MutationOutcome.VALUE_CHANGED,
                () -> "removed",
                () -> {
                },
                () -> {
                }
        );
        YierdisDbMutationExecutor.MutationPlan<String> plan = new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<String> prepare() {
                return prepared;
            }
        };

        org.junit.Assert.assertEquals("removed", preparedExecutor.execute(plan));
        org.junit.Assert.assertEquals(1, preparedLedger.reclamationBegins());
        org.junit.Assert.assertEquals(0, preparedLedger.normalReservations());
    }

    @Test
    public void reclamationRejectsPositiveUpperBoundBeforeCommit() {
        assertRejectedReclamation(1, 0, -1);
    }

    @Test
    public void reclamationRejectsPositiveStagedGrowthBeforeCommit() {
        assertRejectedReclamation(0, 1, -1);
    }

    @Test
    public void reclamationRejectsPositiveActualDeltaBeforeCommit() {
        assertRejectedReclamation(0, 0, 1);
    }

    @Test
    public void reclamationRejectsTransientNativeGrowthBeforeCommit() {
        NativeHandle warmMetadata = preparedAllocator.allocate(NativeObjectKind.STRING_BYTES, 70_000);
        preparedAllocator.free(warmMetadata);
        AtomicBoolean committed = new AtomicBoolean();
        PreparedDbMutation<String> prepared = prepared(
                0,
                0,
                MutationOutcome.NONE,
                () -> {
                    committed.set(true);
                    return "committed";
                },
                () -> {
                },
                () -> {
                }
        );

        IllegalStateException failure = org.junit.Assert.assertThrows(
                IllegalStateException.class,
                () -> preparedExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<String>() {
                    @Override
                    public long upperBoundBytes() {
                        return 0;
                    }

                    @Override
                    public AdmissionMode admissionMode() {
                        return AdmissionMode.RECLAMATION;
                    }

                    @Override
                    public PreparedDbMutation<String> prepare() {
                        NativeHandle temporary = preparedAllocator.allocate(
                                NativeObjectKind.STRING_BYTES,
                                70_000
                        );
                        preparedAllocator.free(temporary);
                        return prepared;
                    }
                })
        );

        org.junit.Assert.assertEquals(
                "reclamation mutation must not stage positive growth",
                failure.getMessage()
        );
        org.junit.Assert.assertFalse(committed.get());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    @Test
    public void legacyReclamationBypassesNormalReservation() {
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(() -> {
        }, preparedLedger);

        String result = executor.execute(new YierdisDbMutationExecutor.LegacyMutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<String> apply() {
                return YierdisDbMutationExecutor.MutationResult.of("removed", 0);
            }
        });

        org.junit.Assert.assertEquals("removed", result);
        org.junit.Assert.assertEquals(1, preparedLedger.reclamationBegins());
        org.junit.Assert.assertEquals(0, preparedLedger.normalReservations());
    }

    @Test
    public void legacyNonCapacityOffHeapFailureIsNotMappedToOom() {
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(() -> {
        }, preparedLedger);
        OffHeapOutOfMemoryException failure = new OffHeapOutOfMemoryException("not capacity");

        OffHeapOutOfMemoryException thrown = org.junit.Assert.assertThrows(
                OffHeapOutOfMemoryException.class,
                () -> executor.execute(new YierdisDbMutationExecutor.LegacyMutationPlan<Void>() {
                    @Override
                    public long upperBoundBytes() {
                        return 0;
                    }

                    @Override
                    public YierdisDbMutationExecutor.MutationResult<Void> apply() {
                        throw failure;
                    }
                })
        );

        org.junit.Assert.assertSame(failure, thrown);
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    @Test
    public void cleanupErrorDoesNotPreventReservationRollback() {
        PreparedDbMutation<String> prepared = prepared(
                0,
                PREPARED_TEST_UPPER_BOUND_BYTES + 1L,
                MutationOutcome.NONE,
                () -> "unused",
                () -> {
                },
                () -> {
                    throw new AssertionError("abort failed");
                }
        );

        IllegalStateException failure = org.junit.Assert.assertThrows(
                IllegalStateException.class,
                () -> preparedExecutor.execute(plan(PREPARED_TEST_UPPER_BOUND_BYTES, prepared))
        );

        org.junit.Assert.assertEquals("prepared mutation exceeded its reservation", failure.getMessage());
        org.junit.Assert.assertEquals(1, failure.getSuppressed().length);
        org.junit.Assert.assertEquals("abort failed", failure.getSuppressed()[0].getMessage());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    @Test
    public void invalidLedgerDeltaIsRejectedBeforeVisibility() {
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean aborted = new AtomicBoolean();
        PreparedDbMutation<String> prepared = prepared(
                -1,
                0,
                MutationOutcome.VALUE_CHANGED,
                () -> {
                    committed.set(true);
                    return "committed";
                },
                () -> {
                },
                () -> aborted.set(true)
        );

        org.junit.Assert.assertThrows(
                IllegalStateException.class,
                () -> preparedExecutor.execute(plan(PREPARED_TEST_UPPER_BOUND_BYTES, prepared))
        );
        org.junit.Assert.assertFalse(committed.get());
        org.junit.Assert.assertTrue(aborted.get());
        org.junit.Assert.assertEquals(0L, preparedLedger.usedBytes());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    private void assertRejectedReclamation(
            long upperBoundBytes,
            long stagedNonNativeGrowthBytes,
            long actualDeltaBytes
    ) {
        AtomicBoolean aborted = new AtomicBoolean();
        PreparedDbMutation<String> prepared = prepared(
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                MutationOutcome.VALUE_CHANGED,
                () -> "committed",
                () -> {
                },
                () -> aborted.set(true)
        );
        YierdisDbMutationExecutor.MutationPlan<String> plan = new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBoundBytes;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<String> prepare() {
                return prepared;
            }
        };

        org.junit.Assert.assertThrows(IllegalStateException.class, () -> preparedExecutor.execute(plan));
        org.junit.Assert.assertTrue(aborted.get());
        org.junit.Assert.assertEquals(0L, preparedLedger.usedBytes());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    private static <T> PreparedDbMutation<T> prepared(
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            Supplier<T> commit,
            Runnable releaseSuperseded,
            Runnable abort
    ) {
        return new AbstractPreparedMutation<T>(actualDeltaBytes, stagedNonNativeGrowthBytes, outcome) {
            @Override
            protected T commitPrepared() {
                return commit.get();
            }

            @Override
            protected void releaseSupersededPrepared() {
                releaseSuperseded.run();
            }

            @Override
            protected void abortPrepared() {
                abort.run();
            }
        };
    }

    private static <T> YierdisDbMutationExecutor.MutationPlan<T> plan(
            long upperBoundBytes,
            PreparedDbMutation<T> prepared
    ) {
        return new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBoundBytes;
            }

            @Override
            public PreparedDbMutation<T> prepare() {
                return prepared;
            }
        };
    }

    @Test
    public void failedMutationRollsBackReservationAndDoesNotPoisonNextMutation() {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(1024, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(db);

            try {
                executor.execute(new YierdisDbMutationExecutor.LegacyMutationPlan<Void>() {
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
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(1, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(db);
            boolean[] mutated = new boolean[]{false};

            try {
                executor.execute(new YierdisDbMutationExecutor.LegacyMutationPlan<Void>() {
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
