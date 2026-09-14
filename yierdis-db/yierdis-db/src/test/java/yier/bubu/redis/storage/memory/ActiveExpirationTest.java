package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.view;

public class ActiveExpirationTest {
    @Test
    public void cleanupExpiredRemovesImmediatelyExpiredKeysWithoutAccess() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        Assert.assertEquals(1, db.size());

        db.cleanupExpired();
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void staleExpirationCandidateIsRejectedAfterRecordChanges() {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] key = b("stale");
            db.strings().setString(key, b("value"), SetMode.NORMAL, ExpireOption.px(0));
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            AllocatorKeyHandle keyHandle = lifecycle.keyHandle(key);
            EntryRecord candidate = lifecycle.entryRecord(keyHandle);

            makePersistentWithoutStartingAnotherMutation(db, key);

            Assert.assertFalse(lifecycle.isCurrentExpiredCandidate(
                    key,
                    keyHandle,
                    candidate,
                    Long.MAX_VALUE
            ));
            Assert.assertEquals(1, db.size());
            Assert.assertEquals(0, db.memoryStats().expireCount());
            Assert.assertArrayEquals(b("value"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
            Assert.assertEquals(-1L, db.ttl().ttlMillis(viewOf(key)));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void staleExpiresIndexEntriesAreSkippedByLazyValidation() {
        // 索引允许 stale 项（re-SET、PERSIST、overwrite、删除重建后旧项不主动清除）：
        // 到期索引项必须按 entry 真实 expireAtMillis 与 identity 惰性校验，stale 项只丢弃不误删。
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            long before = System.currentTimeMillis();

            // re-SET 改 TTL：旧索引项先到期，新 deadline 未到的 key 不得被误删。
            db.strings().setString(b("resettled"), b("v1"), SetMode.NORMAL, ExpireOption.px(60_000));
            db.strings().setString(b("resettled"), b("v2"), SetMode.NORMAL, ExpireOption.px(120_000));
            db.cleanupExpired(before + 65_000L);
            Assert.assertArrayEquals(b("v2"), OwnedReplyValueAssertions.stringValue(db.strings(), b("resettled")));
            db.cleanupExpired(before + 125_000L);
            Assert.assertNull(db.keyLifecycle().entryRecord(b("resettled")));

            // PERSIST：旧索引项到期后 key 必须按 persistent 存活。
            db.strings().setString(b("persisted"), b("v"), SetMode.NORMAL, ExpireOption.px(60_000));
            Assert.assertTrue(db.ttl().persist(viewOf(b("persisted"))).value());
            db.cleanupExpired(Long.MAX_VALUE);
            Assert.assertArrayEquals(b("v"), OwnedReplyValueAssertions.stringValue(db.strings(), b("persisted")));
            Assert.assertEquals(-1L, db.ttl().ttlMillis(viewOf(b("persisted"))));

            // overwrite 清 TTL：旧索引项到期后 key 必须按无 TTL 存活。
            db.strings().setString(b("overwritten"), b("v1"), SetMode.NORMAL, ExpireOption.px(60_000));
            db.strings().setString(b("overwritten"), b("v2"), SetMode.NORMAL, null);
            db.cleanupExpired(Long.MAX_VALUE);
            Assert.assertArrayEquals(b("v2"), OwnedReplyValueAssertions.stringValue(db.strings(), b("overwritten")));

            // 删除后按不同 deadline 重建：旧索引项判 stale，新 deadline 未到不删。
            db.strings().setString(b("recreated"), b("v1"), SetMode.NORMAL, ExpireOption.px(60_000));
            Assert.assertTrue(db.keyspace().del(List.of(b("recreated"))).value() == 1L);
            db.strings().setString(b("recreated"), b("v2"), SetMode.NORMAL, ExpireOption.px(120_000));
            db.cleanupExpired(before + 65_000L);
            Assert.assertArrayEquals(b("v2"), OwnedReplyValueAssertions.stringValue(db.strings(), b("recreated")));

            // 纯删除：消费到 stale native 句柄的索引项也不得抛出异常。
            db.strings().setString(b("deleted"), b("v"), SetMode.NORMAL, ExpireOption.px(60_000));
            Assert.assertTrue(db.keyspace().del(List.of(b("deleted"))).value() == 1L);
            int sizeBefore = db.size();
            db.cleanupExpired(before + 65_000L);
            Assert.assertEquals(sizeBefore, db.size());
            Assert.assertEquals(1, db.memoryStats().expireCount());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredEventuallyRemovesManyExpiredKeysWithoutAccess() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        int n = 200;
        for (int i = 0; i < n; i++) {
            byte[] key = b("k" + i);
            db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        }
        Assert.assertEquals(n, db.size());

        for (int i = 0; i < 100 && db.size() > 0; i++) {
            db.cleanupExpired();
        }
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void maintenanceTickDrainsShortTtlChurnBetweenTicks() {
        // 短 TTL churn：每个维护节拍必须把到期 key 全部排空，物理内存不随轮次无限增长。
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            long usedAfterFirstTick = -1L;
            for (int round = 0; round < 10; round++) {
                for (int i = 0; i < 500; i++) {
                    db.strings().setString(
                            b("churn-" + round + "-" + i),
                            b("v"),
                            SetMode.NORMAL,
                            ExpireOption.px(0)
                    );
                }
                db.runMaintenance();
                Assert.assertEquals("round " + round + " must drain fully", 0, db.size());
                Assert.assertEquals(0, db.memoryStats().expireCount());
                if (round == 0) {
                    usedAfterFirstTick = db.usedBytesForMaxmemory();
                }
            }
            // 稳态下每轮 churn 的物理账必须回到第一轮节拍后的水平附近，不允许逐轮累积。
            Assert.assertTrue(
                    "physical usage must stay bounded across churn rounds",
                    db.usedBytesForMaxmemory() <= usedAfterFirstTick + 65_536L
            );
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void writeAdmissionUnderAllkeysLruReclaimsExpiredKeysInsteadOfOom() {
        // 新 maxmemory 契约：local maxmemory 下过期的 B 仍占内存时，LRU admission 把 B 作为 reclaim
        // victim 回收而不是 OOM；存活 key A 不得被当作 eviction victim。
        byte[] value = new byte[2048];
        final long limitBytes;
        YierdisDb probe = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            probe.strings().setString(b("a"), value, SetMode.NORMAL, null);
            probe.strings().setString(b("b"), value, SetMode.NORMAL, ExpireOption.px(0));
            long usedAfterAB = probe.usedBytesForMaxmemory();
            probe.strings().setString(b("c"), value, SetMode.NORMAL, null);
            long usedAfterABC = probe.usedBytesForMaxmemory();
            Assert.assertTrue(usedAfterABC > usedAfterAB);
            limitBytes = usedAfterABC - 1L;
        } finally {
            probe.shutdown();
        }

        YierdisDb db = TestDbSupport.open(
                limitBytes,
                MaxmemoryPolicy.ALLKEYS_LRU,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            db.strings().setString(b("a"), value, SetMode.NORMAL, null);
            db.strings().setString(b("b"), value, SetMode.NORMAL, ExpireOption.px(0));
            // 过期 key 物理驻留，等待 admission 抽样回收。
            Assert.assertNotNull(db.keyLifecycle().entryRecord(b("b")));

            Assert.assertTrue(db.strings().setString(b("c"), value, SetMode.NORMAL, null).value());

            // admission 回收了过期 key；存活 key 未被误淘汰；物理占用（强制口径）回到 limit 内。
            Assert.assertNull(db.keyLifecycle().entryRecord(b("b")));
            Assert.assertArrayEquals(value, OwnedReplyValueAssertions.stringValue(db.strings(), b("a")));
            Assert.assertArrayEquals(value, OwnedReplyValueAssertions.stringValue(db.strings(), b("c")));
            Assert.assertEquals(2, db.size());
            Assert.assertTrue(db.usedBytesForMaxmemory() <= limitBytes);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void writeAdmissionUnderAllkeysLruSamplingReclaimsExpiredKeys() {
        // samples < keyCount 的随机抽样路径：keyspace 只剩过期 key 时，任何样本都是 reclaim victim，
        // 写入必须通过回收过期占用成功，而不是因为抽不到 live victim 而无故 OOM。
        byte[] value = new byte[2048];
        final long limitBytes;
        List<byte[]> expiredKeys = List.of(b("b1"), b("b2"), b("b3"), b("b4"));
        YierdisDb probe = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (byte[] key : expiredKeys) {
                probe.strings().setString(key, value, SetMode.NORMAL, ExpireOption.px(0));
            }
            long usedAfterExpired = probe.usedBytesForMaxmemory();
            probe.strings().setString(b("c"), value, SetMode.NORMAL, null);
            long usedAfterAll = probe.usedBytesForMaxmemory();
            Assert.assertTrue(usedAfterAll > usedAfterExpired);
            limitBytes = usedAfterAll - 1L;
        } finally {
            probe.shutdown();
        }

        YierdisDb db = TestDbSupport.open(
                limitBytes,
                MaxmemoryPolicy.ALLKEYS_LRU,
                1,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (byte[] key : expiredKeys) {
                db.strings().setString(key, value, SetMode.NORMAL, ExpireOption.px(0));
            }

            Assert.assertTrue(db.strings().setString(b("c"), value, SetMode.NORMAL, null).value());

            Assert.assertArrayEquals(value, OwnedReplyValueAssertions.stringValue(db.strings(), b("c")));
            Assert.assertTrue("admission must reclaim at least one expired key", db.size() <= expiredKeys.size());
            Assert.assertTrue(db.usedBytesForMaxmemory() <= limitBytes);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void writeAdmissionUnderAllkeysRandomReclaimsExpiredKeys() {
        // RANDOM 不回归：全过期 keyspace 中任何随机 victim 都走 expiration reclamation，写入成功。
        byte[] value = new byte[2048];
        final long limitBytes;
        List<byte[]> expiredKeys = List.of(b("b1"), b("b2"), b("b3"), b("b4"));
        YierdisDb probe = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (byte[] key : expiredKeys) {
                probe.strings().setString(key, value, SetMode.NORMAL, ExpireOption.px(0));
            }
            probe.strings().setString(b("c"), value, SetMode.NORMAL, null);
            limitBytes = probe.usedBytesForMaxmemory() - 1L;
        } finally {
            probe.shutdown();
        }

        YierdisDb db = TestDbSupport.open(
                limitBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (byte[] key : expiredKeys) {
                db.strings().setString(key, value, SetMode.NORMAL, ExpireOption.px(0));
            }

            Assert.assertTrue(db.strings().setString(b("c"), value, SetMode.NORMAL, null).value());

            Assert.assertArrayEquals(value, OwnedReplyValueAssertions.stringValue(db.strings(), b("c")));
            Assert.assertTrue("admission must reclaim at least one expired key", db.size() <= expiredKeys.size());
            Assert.assertTrue(db.usedBytesForMaxmemory() <= limitBytes);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void writeAdmissionUnderNoevictionStillRejectsWhenOnlyExpiredOccupancyRemains() {
        // noeviction 永不选 victim：过期占用只能由维护节拍或读路径惰性过期回收，
        // admission 仍按 OOM 拒绝增长写入（与 Redis noeviction 一致）。
        byte[] value = new byte[2048];
        final long limitBytes;
        YierdisDb probe = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            probe.strings().setString(b("a"), value, SetMode.NORMAL, null);
            probe.strings().setString(b("b"), value, SetMode.NORMAL, ExpireOption.px(0));
            long usedAfterAB = probe.usedBytesForMaxmemory();
            probe.strings().setString(b("c"), value, SetMode.NORMAL, null);
            long usedAfterABC = probe.usedBytesForMaxmemory();
            Assert.assertTrue(usedAfterABC > usedAfterAB);
            limitBytes = usedAfterABC - 1L;
        } finally {
            probe.shutdown();
        }

        YierdisDb db = TestDbSupport.open(
                limitBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            db.strings().setString(b("a"), value, SetMode.NORMAL, null);
            db.strings().setString(b("b"), value, SetMode.NORMAL, ExpireOption.px(0));

            try {
                db.strings().setString(b("c"), value, SetMode.NORMAL, null);
                Assert.fail("noeviction admission must not reclaim the expired key inline");
            } catch (YierdisCommandException expected) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
            }
            // 过期 key 物理驻留：noeviction admission 没有顺手回收它。
            Assert.assertNotNull(db.keyLifecycle().entryRecord(b("b")));
            Assert.assertEquals(2, db.size());

            db.runMaintenance();
            Assert.assertNull(db.keyLifecycle().entryRecord(b("b")));

            Assert.assertTrue(db.strings().setString(b("c"), value, SetMode.NORMAL, null).value());
            Assert.assertEquals(2, db.size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredCapsCandidatesAndAdvancesAcrossCalls() {
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (int i = 0; i < 45; i++) {
                db.strings().setString(
                        b("bounded-" + i),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(0)
                );
            }

            db.cleanupExpired();

            Assert.assertEquals(25, db.size());
            Assert.assertEquals(25, db.memoryStats().expireCount());
            for (int i = 0; i < 10 && db.size() > 0; i++) {
                db.cleanupExpired();
            }
            Assert.assertEquals(0, db.size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredDeduplicatesRehashShadowCandidates() {
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            NativeKeyDirectory directory = KeyLifecycleTestAccess.inspect(db.keyLifecycle()).keyDirectory();
            int inserted = 0;
            while (inserted < 200) {
                db.strings().setString(
                        b("rehash-expired-" + inserted),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(0)
                );
                inserted++;
                if (!directory.metrics().rehashing()) {
                    continue;
                }
                if (directory.metrics().oldCapacity() >= 32) {
                    break;
                }
                drainDirectoryRehash(directory);
            }
            Assert.assertTrue(directory.metrics().rehashing());
            int oldCapacity = directory.metrics().oldCapacity();
            directory.advanceRehash(HashTableWorkBudget.of(oldCapacity / 2L, Long.MAX_VALUE));
            Assert.assertTrue(directory.metrics().rehashing());
            Assert.assertTrue(countDuplicateScanIdentities(db) > 0);

            int sizeBeforeCleanup = db.size();

            db.cleanupExpired(Long.MAX_VALUE);

            Assert.assertEquals(20, sizeBeforeCleanup - db.size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredDrainsAcrossDirectoryRehashGenerations() {
        // expires 索引与 key 目录拓扑解耦：rehash 进行中重复调用 cleanup，过期 key 仍全部回收，
        // 不丢、不重；单次调用的候选上限保持不变。
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            NativeKeyDirectory directory = KeyLifecycleTestAccess.inspect(db.keyLifecycle()).keyDirectory();
            List<byte[]> expiredKeys = new ArrayList<>();
            int inserted = 0;
            while (inserted < 200) {
                byte[] key = b("rehash-drained-" + inserted);
                expiredKeys.add(key);
                db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
                inserted++;
                if (directory.metrics().rehashing() && directory.metrics().oldCapacity() >= 32) {
                    break;
                }
                drainDirectoryRehash(directory);
            }
            Assert.assertTrue(directory.metrics().rehashing());
            int sizeBeforeCleanup = db.size();

            db.cleanupExpired(Long.MAX_VALUE);
            Assert.assertEquals(20, sizeBeforeCleanup - db.size());

            // 首次清理在 rehash 中途执行；后续每次删除都会推进 rehash，清理跨越表切换全过程。
            for (int i = 0; i < 20 && db.size() > 0; i++) {
                db.cleanupExpired(Long.MAX_VALUE);
            }

            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0, db.memoryStats().expireCount());
            for (byte[] key : expiredKeys) {
                Assert.assertNull(db.keyLifecycle().entryRecord(key));
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredDoesNotDeleteUnexpiredKeys() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));

        db.cleanupExpired();

        Assert.assertEquals(1, db.size());
        Assert.assertArrayEquals(b("v"), OwnedReplyValueAssertions.stringValue(db.strings(), key));

        db.shutdown();
    }

    @Test
    public void cleanupExpiredVisitsOnlyExpiredKeysAmongLargePersistentKeyspace() {
        // 过期候选来自 expires 索引而不是全 keyspace 游标扫描：1 万 persistent key 中散布的 20 个过期 key
        // 必须在单次 cleanup 调用内全部回收；游标扫描单次最多检查 320 个 slot，无法稳定做到这一点。
        YierdisDb db = TestDbSupport.openWithNativeSlotCapacity(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE,
                null,
                65_536
        );
        try {
            int persistentKeys = 10_000;
            for (int i = 0; i < persistentKeys; i++) {
                db.strings().setString(b("live-" + i), b("v"), SetMode.NORMAL, null);
            }
            List<byte[]> expiredKeys = new ArrayList<>(20);
            for (int i = 0; i < 20; i++) {
                byte[] key = b("expired-" + i);
                expiredKeys.add(key);
                db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
            }

            db.cleanupExpired();

            Assert.assertEquals(persistentKeys, db.size());
            Assert.assertEquals(0, db.memoryStats().expireCount());
            for (byte[] key : expiredKeys) {
                Assert.assertNull(db.keyLifecycle().entryRecord(key));
            }
            Assert.assertArrayEquals(b("v"), OwnedReplyValueAssertions.stringValue(db.strings(), b("live-0")));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void ttlBytesViewLazilyDeletesExpiredKeys() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        Assert.assertEquals(1, db.size());

        BytesView view = viewOf(key);

        Assert.assertEquals(-2L, db.ttl().ttlSeconds(view));
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void deadlineOnlyMutationReusesStoredKeyAndDoesNotChangePhysicalMemoryAccounting() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        byte[] equalLookupKey = b("k");
        Assert.assertNotSame(key, equalLookupKey);
        db.strings().setString(key, b("v"), SetMode.NORMAL, null);
        long usedBeforeTtl = db.usedBytesForMaxmemory();
        BytesView keyView = viewOf(equalLookupKey);

        Assert.assertTrue(db.ttl().expire(keyView, 60).value());
        long usedAfterTtl = db.usedBytesForMaxmemory();
        Assert.assertEquals(usedBeforeTtl, usedAfterTtl);
        Assert.assertEquals(1, db.memoryStats().expireCount());

        Assert.assertTrue(db.ttl().expire(keyView, 120).value());
        Assert.assertEquals(usedAfterTtl, db.usedBytesForMaxmemory());
        Assert.assertTrue(db.ttl().persist(keyView).value());
        long usedAfterPersist = db.usedBytesForMaxmemory();
        Assert.assertEquals(0, db.memoryStats().expireCount());
        Assert.assertEquals(usedBeforeTtl, usedAfterPersist);

        db.shutdown();
    }

    @Test
    public void pexpireBytesViewReadsOnlyTheLookupKey() {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] targetKey = b("bytes-view-target");
            db.strings().setString(targetKey, b("v"), SetMode.NORMAL, null);
            for (int i = 0; i < 256; i++) {
                db.strings().setString(
                        b("bytes-view-dummy-" + i),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(60_000)
                );
            }
            int[] reads = new int[1];
            BytesView view = new BytesView() {
                @Override
                public int length() {
                    return targetKey.length;
                }

                @Override
                public byte getByte(int index) {
                    reads[0]++;
                    return targetKey[index];
                }
            };

            Assert.assertTrue(db.ttl().pexpire(view, 60_000L).value());

            Assert.assertEquals(targetKey.length, reads[0]);
            Assert.assertEquals(257, db.memoryStats().expireCount());
            Assert.assertTrue(db.ttl().ttlMillis(viewOf(targetKey)) > 0L);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredNowMillisHonorsArgument() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));
        Assert.assertEquals(1, db.size());

        long now = System.currentTimeMillis();
        long farFuture = now + 120_000L;
        db.cleanupExpired(farFuture);

        Assert.assertEquals(0, db.size());
        db.shutdown();
    }

    private static BytesView viewOf(byte[] data) {
        return view(data);
    }

    private static void makePersistentWithoutStartingAnotherMutation(YierdisDb db, byte[] key) {
        YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
        AllocatorKeyHandle keyHandle = lifecycle.keyHandle(key);
        EntryHandle entryHandle = lifecycle.entryHandle(key);
        EntryRecord record = lifecycle.entryRecord(entryHandle);
        Assert.assertNotNull(keyHandle);
        Assert.assertNotNull(entryHandle);
        Assert.assertNotNull(record);
        lifecycle.replaceEntry(entryHandle, record, lifecycle.withExpireAtMillis(keyHandle, record, -1L));
    }

    private static void drainDirectoryRehash(NativeKeyDirectory directory) {
        while (directory.metrics().rehashing()) {
            directory.advanceRehash(HashTableWorkBudget.of(Long.MAX_VALUE, Long.MAX_VALUE));
        }
    }

    private static int countDuplicateScanIdentities(YierdisDb db) {
        Set<NativeHandle> identities = new HashSet<>();
        int[] visibleEntries = new int[1];
        YierdisDbKeyLifecycle.KeyScanResult result = db.keyLifecycle().scanWithWork(
                ScanCursorV2.start(),
                Long.MAX_VALUE,
                (key, record) -> {
                    visibleEntries[0]++;
                    identities.add(record.keyHandle());
                    return true;
                }
        );
        Assert.assertEquals(0L, result.nextCursor().value());
        return visibleEntries[0] - identities.size();
    }

}
