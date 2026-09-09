package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

/** 为子包测试组装生产 mutation executor，不向生产 API 增加兼容构造器。 */
public final class MutationExecutorTestSupport {
    private static final long TEST_UPPER_BOUND_BYTES = 1_000_000L;

    private MutationExecutorTestSupport() {
    }

    public static YierdisDbMutationExecutor create(YierdisDb db) {
        return new YierdisDbMutationExecutor(
                db::checkThread,
                db.memoryLedger(),
                KeyLifecycleTestAccess.backend(db),
                db.healthMonitor()
        );
    }

    /**
     * 注入一次 post-commit invariant failure：替换已存在 string key 的 value，
     * commit 后释放旧 value 时抛出 NativeMemoryException，executor settle 账本并把 DB 标记为 degraded。
     */
    public static void replaceStringAndFailDuringRelease(
            YierdisDb db,
            byte[] key,
            byte[] nextBytes,
            String failureMessage
    ) {
        YierdisDbKeyLifecycle keyLifecycle = db.keyLifecycle();
        YierdisDbMutationExecutor executor = create(db);
        executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
            @Override
            public long upperBoundBytes() {
                return TEST_UPPER_BOUND_BYTES;
            }

            @Override
            public PreparedDbMutation<Void> prepare() {
                EntryHandle existingEntryHandle = keyLifecycle.entryHandle(key);
                AllocatorKeyHandle keyHandle = keyLifecycle.keyHandle(key);
                EntryRecord oldRecord = keyLifecycle.entryRecord(existingEntryHandle);
                ValueHandle replacement = KeyLifecycleTestAccess.inspect(keyLifecycle).stringRoot().store(nextBytes);
                EntryRecord newRecord = stringRecord(keyLifecycle, keyHandle, replacement, oldRecord);
                return PreparedEntryMutation.<Void>replace(
                        keyLifecycle,
                        null,
                        0L,
                        0L,
                        existingEntryHandle,
                        oldRecord,
                        newRecord,
                        true
                ).releaseReplacedValueWith(() -> {
                            keyLifecycle.releaseValue(oldRecord);
                            throw new NativeMemoryException(failureMessage);
                        });
            }
        });
    }

    private static EntryRecord stringRecord(
            YierdisDbKeyLifecycle keyLifecycle,
            AllocatorKeyHandle keyHandle,
            ValueHandle valueHandle,
            EntryRecord previous
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                valueHandle,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                -1L,
                previous
        );
    }
}
