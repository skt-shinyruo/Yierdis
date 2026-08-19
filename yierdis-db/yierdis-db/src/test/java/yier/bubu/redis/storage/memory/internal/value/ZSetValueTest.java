package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.memory.testkit.FailOnAllocationStableMemoryBackend;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ZSetValueTest {
    @Test
    public void addImmediatelyReportsNewUpdatedAndUnchangedMembers() {
        try (TestBackend runtime = TestBackend.open("zset-immediate-add");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(new ZSetValue.ZAddResult(1, true),
                        zset.add(List.of(b("1"), b("member"))));
                Assert.assertEquals(1, zset.size());
                Assert.assertEquals("1", scoreFor(zset, "member"));

                Assert.assertEquals(new ZSetValue.ZAddResult(0, true),
                        zset.add(List.of(b("2"), b("member"))));
                Assert.assertEquals(1, zset.size());
                Assert.assertEquals("2", scoreFor(zset, "member"));

                Assert.assertEquals(new ZSetValue.ZAddResult(0, false),
                        zset.add(List.of(b("2"), b("member"))));
                Assert.assertEquals(1, zset.size());
                Assert.assertEquals("2", scoreFor(zset, "member"));
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void stagedPackedBuildPlansOneFinalMemberBlockAcrossAllocatorSizeClasses() {
        ArrayList<byte[]> pairs = new ArrayList<>();
        for (int index = 0; index < YierdisEncodingThresholds.ZSET_MAX_LISTPACK_ENTRIES; index++) {
            pairs.add(b(Integer.toString(index)));
            pairs.add(fixedMember(index, YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES));
        }
        ZSetValue.PackedBuildPlan plan = ZSetValue.preparedNewPackedBuildPlan(pairs);
        Assert.assertNotNull(plan);
        Assert.assertEquals(YierdisEncodingThresholds.ZSET_MAX_LISTPACK_ENTRIES, plan.memberCount());
        Assert.assertTrue(plan.encodedBytes() > 8 * 1024);

        try (TestBackend runtime = TestBackend.open("zset-packed-final-block");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.reservePackedForBuild(plan);
                Assert.assertEquals(plan.memberCount(), zset.add(pairs).added());
                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zset.encoding());
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void preparedNoopAddKeepsTheSourceHandleAndCommitsWithoutAllocation() {
        try (TestBackend runtime = TestBackend.open("zset-prepared-noop");
             FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                     runtime.backend()
             );
             ZSetRoot root = new ZSetRoot(allocator, HashSeed.random(), new HashTableMaintenanceRegistry())) {
            allocator.bindToCurrentThread();
            ValueHandle source = root.create();
            root.zadd(source, List.of(b("1"), b("member")));

            ZSetRoot.AddPlan plan = root.planAdd(source, List.of(b("1"), b("member")));
            Assert.assertArrayEquals(new int[0], plan.nativeAllocationSizes());
            allocator.resetAttempts();
            allocator.failOnAllocation(1);
            try (ZSetRoot.PreparedAddResult prepared = root.prepareAdd(plan)) {
                Assert.assertFalse(prepared.changedAny());
                Assert.assertTrue(prepared.stableHandle());
                Assert.assertEquals(source, prepared.handle());
                Assert.assertEquals(0L, prepared.stagedNonNativeGrowthBytes());

                prepared.commit();
                prepared.releaseSuperseded();
                Assert.assertEquals(0L, allocator.allocationAttempts());
            } finally {
                allocator.disableFailures();
            }

            Assert.assertEquals(1, root.size(source));
        }
    }

    @Test
    public void preparedPackedAddRejectsReleaseBeforeCommitAbortReuseAndDoubleCommit() {
        try (TestBackend runtime = TestBackend.open("zset-prepared-state-guards");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(List.of(b("1"), b("member")));
                long liveBefore = allocator.stats().liveObjects();

                ZSetValue.PreparedExistingAdd aborted = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("2"), b("member")))
                );
                Assert.assertTrue(allocator.stats().liveObjects() > liveBefore);
                Assert.assertThrows(IllegalStateException.class, aborted::releaseSuperseded);
                aborted.close();
                aborted.close();
                Assert.assertEquals(liveBefore, allocator.stats().liveObjects());
                Assert.assertThrows(IllegalStateException.class, aborted::commit);
                Assert.assertEquals("1", scoreFor(zset, "member"));

                try (ZSetValue.PreparedExistingAdd committed = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("2"), b("member"))))) {
                    committed.commit();
                    Assert.assertThrows(IllegalStateException.class, committed::commit);
                    committed.releaseSuperseded();
                    committed.releaseSuperseded();
                }
                Assert.assertEquals("2", scoreFor(zset, "member"));
                Assert.assertEquals(liveBefore, allocator.stats().liveObjects());
            } finally {
                zset.close();
            }
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    @Test
    public void closingNoopPreparedAddMakesItTerminalWithoutAllocating() {
        try (TestBackend runtime = TestBackend.open("zset-prepared-noop-abort");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(List.of(b("1"), b("member")));
                long liveBefore = allocator.stats().liveObjects();
                ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("1"), b("member")))
                );

                prepared.close();
                prepared.close();

                Assert.assertEquals(liveBefore, allocator.stats().liveObjects());
                Assert.assertThrows(IllegalStateException.class, prepared::commit);
                Assert.assertThrows(IllegalStateException.class, prepared::releaseSuperseded);
                Assert.assertEquals("1", scoreFor(zset, "member"));
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void preparedAddHeapUpperBoundsCoverPackedAndSkiplistStagingTopology() {
        try (TestBackend runtime = TestBackend.open("zset-staged-heap-bound");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(4, zset.zaddMany(List.of(
                        b("1"), b("a"),
                        b("2"), b("b"),
                        b("3"), b("c"),
                        b("4"), b("d")
                )));

                ZSetValue.ZAddPlan packedPlan = zset.planExistingAdd(List.of(b("5"), b("a")));
                Assert.assertTrue(packedPlan.stagedHeapBytes() >= zset.heapEstimatedBytes());
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(packedPlan)) {
                    Assert.assertEquals(packedPlan.stagedHeapBytes(), prepared.stagedHeapBytes());
                }

                ZSetValue.ZAddPlan conversionPlan = zset.planExistingAdd(List.of(
                        b("5"),
                        fixedMember(9, YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES + 1)
                ));
                long conversionBound = conversionPlan.stagedHeapBytes();
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(conversionPlan)) {
                    prepared.commit();
                    prepared.releaseSuperseded();
                }
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zset.encoding());
                Assert.assertTrue(conversionBound >= zset.heapEstimatedBytes()
                        + ZSkipList.preparedInsertWorkspaceHeapUpperBound(zset.size()));

                ZSetValue.ZAddPlan deltaPlan = zset.planExistingAdd(List.of(
                        b("6"), b("a"),
                        b("7"), b("new-member")
                ));
                Assert.assertTrue(deltaPlan.stagedHeapBytes()
                        >= ZSkipList.preparedMutationHeapUpperBound(2, 1));
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(deltaPlan)) {
                    Assert.assertEquals(deltaPlan.stagedHeapBytes(), prepared.stagedHeapBytes());
                }
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void skiplistDeltaReusesCanonicalMemberHandlesAndCommitDoesNotAllocate() {
        try (TestBackend runtime = TestBackend.open("zset-skiplist-delta");
             FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                     runtime.backend()
             )) {
            allocator.bindToCurrentThread();
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(skiplistPairs(200));
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zset.encoding());
                Set<Long> beforeHandles = nativeHandles(zset);
                long updatedHandle = nativeHandleFor(allocator, zset, b("m42"));

                ZSetValue.ZAddPlan plan = zset.planExistingAdd(List.of(
                        b("999"), b("m42"),
                        b("1000"), b("m-new"),
                        b("1001"), b("m42")
                ));
                Assert.assertEquals(1, plan.added());
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(plan)) {
                    allocator.resetAttempts();
                    allocator.failOnAllocation(1);

                    prepared.commit();
                    Assert.assertEquals(0L, allocator.allocationAttempts());
                    prepared.releaseSuperseded();
                } finally {
                    allocator.disableFailures();
                }

                Assert.assertEquals(201, zset.size());
                Assert.assertEquals(updatedHandle, nativeHandleFor(allocator, zset, b("m42")));
                Set<Long> afterHandles = nativeHandles(zset);
                Assert.assertTrue(afterHandles.containsAll(beforeHandles));
                Assert.assertEquals("1001", scoreFor(zset, "m42"));
            } finally {
                allocator.disableFailures();
                zset.close();
            }
        }
    }

    @Test
    public void abortingSkiplistDeltaReleasesOnlyTheNewCanonicalMember() {
        try (TestBackend runtime = TestBackend.open("zset-skiplist-abort");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(skiplistPairs(200));
                Set<Long> beforeHandles = nativeHandles(zset);

                try (ZSetValue.PreparedExistingAdd ignored = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("500"), b("abort-member"))))) {
                    Assert.assertEquals(201, nativeHandles(zset).size() + 1);
                }

                Assert.assertEquals(200, zset.size());
                Assert.assertEquals(beforeHandles, nativeHandles(zset));
                Assert.assertNull(scoreFor(zset, "abort-member"));
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void packedCommitReclaimsTheOldBlockWhenHeapRefreshFails() {
        try (TestBackend runtime = TestBackend.open("zset-packed-refresh-failure");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(List.of(b("1"), b("member")));
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("2"), b("member"))))) {
                    zset.setHeapChangeListener(() -> {
                        throw new IllegalStateException("injected heap refresh failure");
                    });

                    prepared.commit();
                    Assert.assertThrows(IllegalStateException.class, prepared::releaseSuperseded);
                    Assert.assertEquals("2", scoreFor(zset, "member"));

                    zset.setHeapChangeListener(() -> {
                    });
                    prepared.releaseSuperseded();
                }
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void preparedPackedAddRejectsAnUnrelatedInPlaceMutation() {
        try (TestBackend runtime = TestBackend.open("zset-packed-stale-plan");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(List.of(b("1"), b("a"), b("2"), b("b")));
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("3"), b("a"))))) {
                    Assert.assertEquals(1, zset.zrem(List.of(b("b"))));
                    Assert.assertThrows(IllegalStateException.class, prepared::commit);
                }

                Assert.assertEquals("1", scoreFor(zset, "a"));
                Assert.assertNull(scoreFor(zset, "b"));
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void preparedSkiplistAddRejectsAnUnrelatedScoreMutationBeforePublishing() {
        try (TestBackend runtime = TestBackend.open("zset-skiplist-stale-plan");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(skiplistPairs(200));
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("500"), b("m0"))))) {
                    zset.zaddMany(List.of(b("600"), b("m1")));
                    Assert.assertThrows(IllegalStateException.class, prepared::commit);
                }

                Assert.assertEquals("0", scoreFor(zset, "m0"));
                Assert.assertEquals("600", scoreFor(zset, "m1"));
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void signedZeroIsOneScoreInPackedAndSkiplistEncodings() {
        assertSignedZeroSemantics(new ZSetValueFactory("zset-signed-zero-packed", false));
        assertSignedZeroSemantics(new ZSetValueFactory("zset-signed-zero-skiplist", true));
    }

    @Test
    public void packedZSetKeepsScoreOrderingAndSupportsUpdates() {
        try (TestBackend runtime = TestBackend.open("zset-test");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zv = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zv.encoding());

                List<byte[]> args = Arrays.asList(
                        b("1"), b("a"),
                        b("1"), b("b"),
                        b("0"), b("c")
                );
                Assert.assertEquals(3, zv.zaddMany(args));

                List<byte[]> range = zv.zrange(0, -1, false);
                Assert.assertEquals(3, range.size());
                Assert.assertArrayEquals(b("c"), range.get(0));
                Assert.assertArrayEquals(b("a"), range.get(1));
                Assert.assertArrayEquals(b("b"), range.get(2));

                Assert.assertEquals(0, zv.zaddMany(Arrays.asList(b("2"), b("a"))));
                List<byte[]> range2 = zv.zrange(0, -1, false);
                Assert.assertEquals(3, range2.size());
                Assert.assertArrayEquals(b("c"), range2.get(0));
                Assert.assertArrayEquals(b("b"), range2.get(1));
                Assert.assertArrayEquals(b("a"), range2.get(2));

                Assert.assertEquals(1, zv.zrem(List.of(b("b"))));
                Assert.assertEquals(2, zv.size());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void packedInsertAllocationFailureLeavesScoresAndMembersAligned() {
        try (TestBackend runtime = TestBackend.open("zset-packed-insert-failure");
             FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                     runtime.backend()
             )) {
            allocator.bindToCurrentThread();
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(2, zset.zaddMany(List.of(b("1"), b("a"), b("3"), b("c"))));
                long usedBefore = allocator.stats().logicalUsedBytes();
                allocator.resetAttempts();
                allocator.failOnAllocation(1);

                try {
                    zset.zaddMany(List.of(b("2"), fixedMember(7, 64)));
                    Assert.fail("expected injected allocation failure");
                } catch (NativeCapacityExceededException expected) {
                    // 失败点位于 member block 扩容，原有 score/member 对应关系必须保持不变。
                }

                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zset.encoding());
                Assert.assertEquals(2, zset.size());
                Assert.assertEquals(List.of("a", "1", "c", "3"), strings(zset.zrange(0, -1, true)));
                Assert.assertEquals(usedBefore, allocator.stats().logicalUsedBytes());
            } finally {
                allocator.disableFailures();
                zset.close();
            }
        }
    }

    @Test
    public void failedPackedToSkiplistConversionReleasesEveryStagedMember() {
        try (TestBackend runtime = TestBackend.open("zset-convert-failure");
             FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                     runtime.backend()
             )) {
            allocator.bindToCurrentThread();
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(4, zset.zaddMany(List.of(
                        b("1"), b("a"),
                        b("2"), b("b"),
                        b("3"), b("c"),
                        b("4"), b("d")
                )));
                long usedBefore = allocator.stats().logicalUsedBytes();
                allocator.resetAttempts();
                allocator.failOnAllocation(5);

                try {
                    zset.zaddMany(List.of(
                            b("5"),
                            fixedMember(9, YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES + 1)
                    ));
                    Assert.fail("expected injected allocation failure");
                } catch (NativeCapacityExceededException expected) {
                    // 已暂存的 canonical member 由 staging 独占；borrowed map 回滚不能重复释放它们。
                }

                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zset.encoding());
                Assert.assertEquals(4, zset.size());
                Assert.assertEquals(List.of("a", "b", "c", "d"), strings(zset.zrange(0, -1, false)));
                Assert.assertEquals(usedBefore, allocator.stats().logicalUsedBytes());
            } finally {
                allocator.disableFailures();
                zset.close();
            }
        }
    }

    @Test
    public void packedToSkiplistConversionPublishesCanonicalMembersWithoutCommitAllocation() {
        try (TestBackend runtime = TestBackend.open("zset-convert-commit");
             FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                     runtime.backend()
             )) {
            allocator.bindToCurrentThread();
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(4, zset.zaddMany(List.of(
                        b("1"), b("a"),
                        b("2"), b("b"),
                        b("3"), b("c"),
                        b("4"), b("d")
                )));
                byte[] largeMember = fixedMember(
                        9,
                        YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES + 1
                );
                ZSetValue.ZAddPlan plan = zset.planExistingAdd(List.of(b("5"), largeMember));
                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(plan)) {
                    Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, prepared.targetEncoding());
                    allocator.resetAttempts();
                    allocator.failOnAllocation(1);

                    prepared.commit();
                    prepared.releaseSuperseded();
                    Assert.assertEquals(0L, allocator.allocationAttempts());
                } finally {
                    allocator.disableFailures();
                }

                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zset.encoding());
                Assert.assertEquals(5, zset.size());
                Assert.assertEquals(5, nativeHandles(zset).size());
                Assert.assertEquals(
                        List.of("a", "b", "c", "d", new String(largeMember, StandardCharsets.US_ASCII)),
                        strings(zset.zrange(0, -1, false))
                );
            } finally {
                allocator.disableFailures();
                zset.close();
            }
        }
    }

    @Test
    public void zsetUpgradesAfterTooManyEntries() {
        try (TestBackend runtime = TestBackend.open("zset-test");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zv = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                Assert.assertEquals(200, zv.size());
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zv.encoding());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void skiplistRangeRemovalUsesMeasuredNativeMemberLookup() {
        try (TestBackend runtime = TestBackend.open("zset-remove-skiplist");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zv = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zv.encoding());

                Assert.assertEquals(100, zv.countRemovalsByRank(50, 149));
                Assert.assertEquals(100, zv.zremrangeByRank(50, 149));
                Assert.assertEquals(100, zv.size());
                Assert.assertEquals(50, zv.countRemovalsByScore(0, false, 49, false));
                Assert.assertEquals(50, zv.zremrangeByScore(0, false, 49, false));
                Assert.assertEquals(50, zv.size());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void preparedCopyHeapUpperBoundCoversListpackToSkiplistUpgrade() {
        try (TestBackend runtime = TestBackend.open("zset-prepared-heap");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue source = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            ZSetValue replacement = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                List<byte[]> sourcePairs = List.of(
                        b("1"), b("alpha"), b("2"), b("beta"), b("3"), b("gamma")
                );
                source.zaddMany(sourcePairs);
                List<byte[]> addition = List.of(b("4"), new byte[256]);
                long upperBound = source.preparedCopyHeapUpperBound(addition);

                replacement.zaddMany(sourcePairs);
                replacement.add(addition);

                Assert.assertTrue(replacement.heapEstimatedBytes() <= upperBound);
            } finally {
                replacement.close();
                source.close();
            }
        }
    }

    @Test
    public void packedZSetStreamsMembersThroughNativeBytesSlice() {
        try (TestBackend runtime = TestBackend.open("zset-stream-packed");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zv = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                Assert.assertEquals(2, zv.zaddMany(List.of(b("1"), b("m1"), b("2"), b("m2"))));
                RecordingSink out = new RecordingSink();

                zv.zrangeWriteTo(0, -1, false, out);

                Assert.assertEquals(List.of("m1", "m2"), out.values);
                Assert.assertTrue(out.sawNativeBytesSlice);
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void skiplistZSetStreamsMembersThroughNativeBytesSlice() {
        try (TestBackend runtime = TestBackend.open("zset-stream-skiplist");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zv = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                RecordingSink out = new RecordingSink();

                zv.zrangeWriteTo(0, 1, false, out);

                Assert.assertEquals(List.of("m0", "m1"), out.values);
                Assert.assertTrue(out.sawNativeBytesSlice);
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void scoreAndRankBoundariesMatchPackedAndSkiplistEncodings() {
        assertScoreAndRankBoundaries(false);
        assertScoreAndRankBoundaries(true);
    }

    @Test
    public void packedScoreRangeViewsShareBoundsPaginationAndDirection() {
        try (TestBackend runtime = TestBackend.open("zset-packed-score-range");
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                zset.zaddMany(scoreBoundaryPairs());
                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zset.encoding());

                assertPackedScoreRangeViews(
                        zset,
                        false,
                        1,
                        false,
                        4,
                        true,
                        1,
                        2,
                        List.of("b", "2", "c", "2")
                );
                assertPackedScoreRangeViews(
                        zset,
                        true,
                        1,
                        true,
                        4,
                        false,
                        1,
                        10,
                        List.of("d", "3", "c", "2", "b", "2")
                );
            } finally {
                zset.close();
            }
        }
    }

    @Test
    public void scoreAndRankRemovalsMatchPackedAndSkiplistEncodings() {
        assertScoreAndRankRemovals(false);
        assertScoreAndRankRemovals(true);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static void assertPackedScoreRangeViews(
            ZSetValue zset,
            boolean reverse,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            long offset,
            long count,
            List<String> expected
    ) {
        List<byte[]> materialized = reverse
                ? zset.zrevrangeByScore(min, minExclusive, max, maxExclusive, true, offset, count)
                : zset.zrangeByScore(min, minExclusive, max, maxExclusive, true, offset, count);
        int elementCount = reverse
                ? zset.zrevrangeByScoreCount(min, minExclusive, max, maxExclusive, true, offset, count)
                : zset.zrangeByScoreCount(min, minExclusive, max, maxExclusive, true, offset, count);
        RecordingSink streamed = new RecordingSink();
        if (reverse) {
            zset.zrevrangeByScoreWriteTo(
                    min, minExclusive, max, maxExclusive, true, offset, count, streamed
            );
        } else {
            zset.zrangeByScoreWriteTo(
                    min, minExclusive, max, maxExclusive, true, offset, count, streamed
            );
        }

        Assert.assertEquals(expected, strings(materialized));
        Assert.assertEquals(expected.size(), elementCount);
        Assert.assertEquals(expected, streamed.values);
    }

    private static byte[] fixedMember(int index, int length) {
        byte[] out = new byte[length];
        Arrays.fill(out, (byte) 'm');
        byte[] suffix = Integer.toString(index).getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(suffix, 0, out, out.length - suffix.length, suffix.length);
        return out;
    }

    private static List<byte[]> skiplistPairs(int count) {
        ArrayList<byte[]> pairs = new ArrayList<>(count * 2);
        for (int index = 0; index < count; index++) {
            pairs.add(b(Integer.toString(index)));
            pairs.add(b("m" + index));
        }
        return pairs;
    }

    private static Set<Long> nativeHandles(ZSetValue zset) {
        Set<Long> handles = new HashSet<>();
        zset.forEachNativeHandle(handle -> handles.add(handle.localRaw()));
        return handles;
    }

    private static long nativeHandleFor(StableMemoryBackend allocator, ZSetValue zset, byte[] expected) {
        long[] found = {0L};
        zset.forEachNativeHandle(handle -> {
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                if (view.size() == expected.length && view.contentEquals(0, expected, 0, expected.length)) {
                    found[0] = handle.localRaw();
                }
            }
        });
        Assert.assertNotEquals("missing native member handle", 0L, found[0]);
        return found[0];
    }

    private static String scoreFor(ZSetValue zset, String member) {
        List<String> values = strings(zset.zrange(0, -1, true));
        for (int index = 0; index < values.size(); index += 2) {
            if (member.equals(values.get(index))) {
                return values.get(index + 1);
            }
        }
        return null;
    }

    private static void assertSignedZeroSemantics(ZSetValueFactory factory) {
        try (TestBackend runtime = TestBackend.open(factory.runtimeName);
             FailOnAllocationStableMemoryBackend allocator = new FailOnAllocationStableMemoryBackend(
                     runtime.backend()
             )) {
            allocator.bindToCurrentThread();
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                if (factory.skiplist) {
                    zset.zaddMany(List.of(
                            b("1"),
                            fixedMember(1, YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES + 1)
                    ));
                }
                Assert.assertEquals(1, zset.zaddMany(List.of(b("-0.0"), b("zero-member"))));
                allocator.resetAttempts();
                allocator.failOnAllocation(1);

                try (ZSetValue.PreparedExistingAdd prepared = zset.prepareExistingAdd(
                        zset.planExistingAdd(List.of(b("+0.0"), b("zero-member"))))) {
                    Assert.assertFalse(prepared.changedAny());
                    prepared.commit();
                    prepared.releaseSuperseded();
                } finally {
                    allocator.disableFailures();
                }

                Assert.assertEquals(0L, allocator.allocationAttempts());
                Assert.assertEquals("0", scoreFor(zset, "zero-member"));
                Assert.assertEquals(
                        List.of("zero-member"),
                        strings(zset.zrangeByScore(-0.0d, false, +0.0d, false, false, 0, 10))
                );
                Assert.assertTrue(zset.zrangeByScore(-0.0d, true, +0.0d, false, false, 0, 10).isEmpty());
            } finally {
                allocator.disableFailures();
                zset.close();
            }
        }
    }

    private static void assertScoreAndRankBoundaries(boolean skiplist) {
        try (TestBackend runtime = TestBackend.open("zset-boundaries-" + skiplist);
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                if (skiplist) {
                    zset.prepareSkiplistForBuild();
                }
                zset.zaddMany(scoreBoundaryPairs());
                Assert.assertEquals(
                        skiplist ? ValueEncoding.ZSET_SKIPLIST : ValueEncoding.ZSET_PACKED,
                        zset.encoding()
                );

                Assert.assertEquals(
                        List.of("d"),
                        strings(zset.zrangeByScore(2, true, 4, true, false, 0, 10))
                );
                Assert.assertEquals(
                        List.of("b", "2", "c", "2"),
                        strings(zset.zrangeByScore(1, false, 4, false, true, 1, 2))
                );
                Assert.assertEquals(
                        List.of("d", "3", "c", "2", "b", "2"),
                        strings(zset.zrevrangeByScore(1, false, 4, false, true, 1, 3))
                );
                Assert.assertTrue(zset.zrangeByScore(1, false, 4, false, false, 0, 0).isEmpty());
                Assert.assertTrue(zset.zrevrangeByScore(1, false, 4, false, false, 0, -1).isEmpty());

                Assert.assertEquals(List.of("e", "d", "c"), strings(zset.zrevrange(0, 2, false)));
                Assert.assertEquals(List.of("d", "e"), strings(zset.zrange(-2, -1, false)));
                Assert.assertEquals(4, zset.zrangeByScoreCount(1, false, 4, false, false, 1, 10));
                Assert.assertEquals(6, zset.zrevrangeByScoreCount(1, false, 4, false, true, 1, 3));

                RecordingSink out = new RecordingSink();
                zset.zrevrangeByScoreWriteTo(1, false, 4, false, true, 1, 2, out);
                Assert.assertEquals(List.of("d", "3", "c", "2"), out.values);

                Assert.assertEquals(2, zset.countExistingMembers(List.of(
                        b("a"), b("a"), b("missing"), b("e")
                )));
            } finally {
                zset.close();
            }
        }
    }

    private static void assertScoreAndRankRemovals(boolean skiplist) {
        try (TestBackend runtime = TestBackend.open("zset-removals-" + skiplist);
             StableMemoryBackend allocator = runtime.backend()) {
            ZSetValue zset = new ZSetValue(allocator, HashSeed.random(), new HashTableMaintenanceRegistry());
            try {
                if (skiplist) {
                    zset.prepareSkiplistForBuild();
                }
                zset.zaddMany(scoreBoundaryPairs());

                Assert.assertEquals(2, zset.countRemovalsByScore(2, true, 4, false));
                Assert.assertEquals(2, zset.zremrangeByScore(2, true, 4, false));
                Assert.assertEquals(List.of("a", "b", "c"), strings(zset.zrange(0, -1, false)));

                Assert.assertEquals(2, zset.countRemovalsByRank(-2, -1));
                Assert.assertEquals(2, zset.zremrangeByRank(-2, -1));
                Assert.assertEquals(List.of("a"), strings(zset.zrange(0, -1, false)));
                Assert.assertEquals(0, zset.countRemovalsByRank(5, 9));
                Assert.assertEquals(0, zset.zremrangeByRank(-1, -2));
            } finally {
                zset.close();
            }
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    private static List<byte[]> scoreBoundaryPairs() {
        return List.of(
                b("1"), b("a"),
                b("2"), b("b"),
                b("2"), b("c"),
                b("3"), b("d"),
                b("4"), b("e")
        );
    }

    private record ZSetValueFactory(String runtimeName, boolean skiplist) {
    }

    private static List<String> strings(List<byte[]> values) {
        ArrayList<String> out = new ArrayList<>(values.size());
        for (byte[] value : values) {
            out.add(value == null ? null : new String(value, StandardCharsets.US_ASCII));
        }
        return out;
    }

    private static final class RecordingSink implements ByteValueSink {
        private final ArrayList<String> values = new ArrayList<>();
        private boolean sawNativeBytesSlice;

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = false;
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = false;
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
            sawNativeBytesSlice = slice instanceof NativeBytesSlice;
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
            sawNativeBytesSlice = false;
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }
    }
}
