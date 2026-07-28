package yier.bubu.redis.storage.memory.internal.ledger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
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
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;

import java.nio.charset.StandardCharsets;

public class MutationExecutorReservationTest {
    private static final long PREPARED_TEST_UPPER_BOUND_BYTES = 1_000_000L;

    private InMemoryLedger preparedLedger;
    private TestBackend preparedRuntime;
    private StableMemoryBackend preparedAllocator;
    private YierdisDbMutationExecutor preparedExecutor;

    @Before
    public void setUpPreparedFixture() {
        preparedLedger = new InMemoryLedger(0);
        preparedRuntime = TestBackend.open("prepared-mutation-test");
        preparedAllocator = preparedRuntime.backend();
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
    public void zeroDeltaOnlyTrimsNativePagesWhenPreparedMutationRequestsIt() {
        AtomicInteger trimCalls = new AtomicInteger();
        StableMemoryBackend recordingAllocator = allocatorThatCountsTrims(trimCalls);
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(
                () -> {
                },
                new InMemoryLedger(PREPARED_TEST_UPPER_BOUND_BYTES * 2L),
                recordingAllocator
        );

        PreparedDbMutation<String> noTrim = prepared(
                0L,
                0L,
                MutationOutcome.NONE,
                () -> "noop",
                () -> {
                },
                () -> {
                }
        );
        executor.execute(plan(PREPARED_TEST_UPPER_BOUND_BYTES, noTrim));
        Assert.assertEquals(0, trimCalls.get());

        PreparedDbMutation<String> requestedTrim = new AbstractPreparedMutation<>(
                0L,
                0L,
                MutationOutcome.VALUE_CHANGED
        ) {
            @Override
            public boolean shouldTrimNativePagesAfterCommit() {
                return true;
            }

            @Override
            protected String commitPrepared() {
                return "changed";
            }

            @Override
            protected void releaseSupersededPrepared() {
            }

            @Override
            protected void abortPrepared() {
            }
        };
        executor.execute(plan(PREPARED_TEST_UPPER_BOUND_BYTES, requestedTrim));

        Assert.assertEquals(1, trimCalls.get());
        Assert.assertTrue(prepared(
                -1L,
                0L,
                MutationOutcome.VALUE_CHANGED,
                () -> "shrink",
                () -> {
                },
                () -> {
                }
        ).shouldTrimNativePagesAfterCommit());
    }

    @Test
    public void postCommitSettlementStillTrimsAfterSupersededReleaseFails() {
        AtomicInteger trimCalls = new AtomicInteger();
        AtomicInteger releaseCalls = new AtomicInteger();
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(
                () -> {
                },
                new InMemoryLedger(PREPARED_TEST_UPPER_BOUND_BYTES * 2L),
                allocatorThatCountsTrims(trimCalls)
        );
        PreparedDbMutation<String> prepared = new AbstractPreparedMutation<>(
                0L,
                0L,
                MutationOutcome.VALUE_CHANGED
        ) {
            @Override
            public boolean shouldTrimNativePagesAfterCommit() {
                return true;
            }

            @Override
            protected String commitPrepared() {
                return "changed";
            }

            @Override
            protected void releaseSupersededPrepared() {
                releaseCalls.incrementAndGet();
                throw new IllegalStateException("release failed");
            }

            @Override
            protected void abortPrepared() {
            }
        };

        Assert.assertThrows(
                PostCommitMutationException.class,
                () -> executor.execute(plan(PREPARED_TEST_UPPER_BOUND_BYTES, prepared))
        );

        Assert.assertEquals(2, releaseCalls.get());
        Assert.assertEquals(1, trimCalls.get());
    }

    @Test
    public void releaseFailureDoesNotRetryAnAlreadyClaimedCallback() {
        AtomicInteger releaseCalls = new AtomicInteger();
        PreparedCallbackMutation<String> prepared = new PreparedCallbackMutation<>(
                "ok",
                0L,
                0L,
                MutationOutcome.NONE,
                () -> {
                },
                () -> {
                    releaseCalls.incrementAndGet();
                    throw new IllegalStateException("release failed");
                },
                () -> {
                }
        );

        prepared.commit();
        org.junit.Assert.assertThrows(IllegalStateException.class, prepared::releaseSuperseded);
        prepared.releaseSuperseded();

        org.junit.Assert.assertEquals(1, releaseCalls.get());
    }

    @Test
    public void reAdmitsWhenAdmissionChangesMutationUpperBound() {
        AtomicLong upperBound = new AtomicLong(10_000L);
        AdmissionChangingLedger ledger = new AdmissionChangingLedger(
                new InMemoryLedger(0),
                () -> upperBound.set(20_000L)
        );
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(
                () -> {
                },
                ledger,
                preparedAllocator
        );
        PreparedDbMutation<String> prepared = prepared(
                0L,
                0L,
                MutationOutcome.NONE,
                () -> "ok",
                () -> {
                },
                () -> {
                }
        );

        org.junit.Assert.assertEquals(
                "ok",
                executor.execute(new YierdisDbMutationExecutor.MutationPlan<String>() {
                    @Override
                    public long upperBoundBytes() {
                        return upperBound.get();
                    }

                    @Override
                    public PreparedDbMutation<String> prepare() {
                        return prepared;
                    }
                })
        );
        org.junit.Assert.assertEquals(2, ledger.normalReservationCalls());
        org.junit.Assert.assertEquals(0L, ledger.reservedBytes());
    }

    @Test
    public void reAdmitsAfterRejectedAdmissionShrinksMutationUpperBound() {
        AtomicLong upperBound = new AtomicLong(20_000L);
        AdmissionFailingLedger ledger = new AdmissionFailingLedger(
                new InMemoryLedger(0),
                () -> upperBound.set(10_000L)
        );
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(
                () -> {
                },
                ledger,
                preparedAllocator
        );
        PreparedDbMutation<String> prepared = prepared(
                0L,
                0L,
                MutationOutcome.NONE,
                () -> "ok",
                () -> {
                },
                () -> {
                }
        );

        org.junit.Assert.assertEquals(
                "ok",
                executor.execute(new YierdisDbMutationExecutor.MutationPlan<String>() {
                    @Override
                    public long upperBoundBytes() {
                        return upperBound.get();
                    }

                    @Override
                    public PreparedDbMutation<String> prepare() {
                        return prepared;
                    }
                })
        );
        org.junit.Assert.assertEquals(2, ledger.normalReservationCalls());
        org.junit.Assert.assertEquals(0L, ledger.reservedBytes());
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
    public void reclamationAcceptsTransientAllocationWhenBackendReportsNoCommittedGrowth() {
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

        String result = preparedExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<String>() {
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
                });

        org.junit.Assert.assertEquals("committed", result);
        org.junit.Assert.assertTrue(committed.get());
        org.junit.Assert.assertEquals(0L, preparedLedger.reservedBytes());
    }

    @Test
    public void commandFailureDuringPreparationDoesNotDegradeDb() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = MutationExecutorTestSupport.create(db);

            WrongTypeException failure = org.junit.Assert.assertThrows(
                    WrongTypeException.class,
                    () -> executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
                        @Override
                        public long upperBoundBytes() {
                            return 0L;
                        }

                        @Override
                        public PreparedDbMutation<Void> prepare() {
                            throw new WrongTypeException();
                        }
                    })
            );

            org.junit.Assert.assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", failure.getMessage());
            org.junit.Assert.assertFalse(db.health().degraded());
            org.junit.Assert.assertEquals(0L, db.memory().memoryStats().reservedBytes());
        } finally {
            db.shutdown();
        }
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

        org.junit.Assert.assertTrue(failure.getMessage().startsWith("prepared mutation exceeded its reservation"));
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

    private StableMemoryBackend allocatorThatCountsTrims(AtomicInteger trimCalls) {
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("trimEmptyPages")) {
                        trimCalls.incrementAndGet();
                    }
                    try {
                        return method.invoke(preparedAllocator, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }
        );
    }

    private static final class AdmissionChangingLedger implements MemoryLedger {
        private final MemoryLedger delegate;
        private final Runnable afterFirstNormalReservation;
        private final AtomicInteger normalReservationCalls = new AtomicInteger();

        private AdmissionChangingLedger(MemoryLedger delegate, Runnable afterFirstNormalReservation) {
            this.delegate = delegate;
            this.afterFirstNormalReservation = afterFirstNormalReservation;
        }

        @Override
        public long limitBytes() {
            return delegate.limitBytes();
        }

        @Override
        public long usedBytes() {
            return delegate.usedBytes();
        }

        @Override
        public long reservedBytes() {
            return delegate.reservedBytes();
        }

        @Override
        public MemoryReservation reserve(long estimatedExtraBytes) {
            MemoryReservation reservation = delegate.reserve(estimatedExtraBytes);
            if (normalReservationCalls.incrementAndGet() == 1) {
                afterFirstNormalReservation.run();
            }
            return reservation;
        }

        @Override
        public void reconcile(MemoryReservation reservation, long requiredBytes) {
            delegate.reconcile(reservation, requiredBytes);
        }

        @Override
        public MemoryReservation beginReclamation() {
            return delegate.beginReclamation();
        }

        @Override
        public void commit(MemoryReservation reservation, long actualDeltaBytes) {
            delegate.commit(reservation, actualDeltaBytes);
        }

        @Override
        public void rollback(MemoryReservation reservation) {
            delegate.rollback(reservation);
        }

        private int normalReservationCalls() {
            return normalReservationCalls.get();
        }
    }

    private static final class AdmissionFailingLedger implements MemoryLedger {
        private final MemoryLedger delegate;
        private final Runnable afterFirstRejectedReservation;
        private final AtomicInteger normalReservationCalls = new AtomicInteger();

        private AdmissionFailingLedger(MemoryLedger delegate, Runnable afterFirstRejectedReservation) {
            this.delegate = delegate;
            this.afterFirstRejectedReservation = afterFirstRejectedReservation;
        }

        @Override
        public long limitBytes() {
            return delegate.limitBytes();
        }

        @Override
        public long usedBytes() {
            return delegate.usedBytes();
        }

        @Override
        public long reservedBytes() {
            return delegate.reservedBytes();
        }

        @Override
        public MemoryReservation reserve(long estimatedExtraBytes) {
            if (normalReservationCalls.incrementAndGet() == 1) {
                afterFirstRejectedReservation.run();
                throw new MemoryLedgerOutOfMemoryException();
            }
            return delegate.reserve(estimatedExtraBytes);
        }

        @Override
        public void reconcile(MemoryReservation reservation, long requiredBytes) {
            delegate.reconcile(reservation, requiredBytes);
        }

        @Override
        public MemoryReservation beginReclamation() {
            return delegate.beginReclamation();
        }

        @Override
        public void commit(MemoryReservation reservation, long actualDeltaBytes) {
            delegate.commit(reservation, actualDeltaBytes);
        }

        @Override
        public void rollback(MemoryReservation reservation) {
            delegate.rollback(reservation);
        }

        private int normalReservationCalls() {
            return normalReservationCalls.get();
        }
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
    public void preparationFailureRollsBackReservationAndDoesNotPoisonNextMutation() {
        YierdisDb db = TestDbSupport.open(
                PREPARED_TEST_UPPER_BOUND_BYTES,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5
        );
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = MutationExecutorTestSupport.create(db);

            try {
                executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
                    @Override
                    public long upperBoundBytes() {
                        return 64;
                    }

                    @Override
                    public PreparedDbMutation<Void> prepare() {
                        throw new WrongTypeException();
                    }
                });
                Assert.fail("expected mutation failure");
            } catch (WrongTypeException expected) {
                Assert.assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", expected.getMessage());
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
    public void noevictionRejectsBeforePreparationCanRun() {
        YierdisDb db = TestDbSupport.open(1, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        db.bindToCurrentThread();
        try {
            YierdisDbMutationExecutor executor = MutationExecutorTestSupport.create(db);
            boolean[] prepared = new boolean[]{false};

            try {
                executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
                    @Override
                    public long upperBoundBytes() {
                        return 64;
                    }

                    @Override
                    public PreparedDbMutation<Void> prepare() {
                        prepared[0] = true;
                        throw new AssertionError("preparation must not run after rejected admission");
                    }
                });
                Assert.fail("expected OOM");
            } catch (YierdisCommandException expected) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
            }

            Assert.assertFalse(prepared[0]);
            Assert.assertNull(db.reads().strings().getStringBytes(bytes("oom")));
            Assert.assertEquals(0L, db.memory().memoryStats().reservedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void appendAndSetbitNoopsDoNotMarkValueChanged() {
        YierdisDb db = TestDbSupport.open();
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
