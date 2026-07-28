package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PayloadLengthSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

/** 使用生产工厂创建 owner-bound 堆后端 DB 的测试入口。 */
public final class TestDbSupport {
    private TestDbSupport() {
    }

    public static YierdisDb open() {
        return open(config());
    }

    public static YierdisDb open(DbEngineConfig config) {
        return openWithFactory(
                HeapStableMemoryBackend::new,
                4096,
                config
        );
    }

    public static YierdisDb open(TestBackend backend, DbEngineConfig config) {
        return openWithFactory(backend, backend.maxSlots(), config);
    }

    static YierdisDb openWithFactory(
            yier.bubu.redis.memory.api.StableMemoryBackendFactory backendFactory,
            int nativeSlotCapacity,
            DbEngineConfig config
    ) {
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                backendFactory,
                new YierdisDbBackendConfig(nativeSlotCapacity)
        );
        YierdisDb db = (YierdisDb) factory.create(config);
        db.bindToCurrentThread();
        return db;
    }

    public static YierdisDb open(
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis
    ) {
        return open(config(
                maxmemoryBytes,
                policy,
                samples,
                evictionLimitMillis,
                expireLimitMillis,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
    }

    public static YierdisDb open(
            TestBackend backend,
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis
    ) {
        return open(backend, config(
                maxmemoryBytes,
                policy,
                samples,
                evictionLimitMillis,
                expireLimitMillis,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
    }

    public static YierdisDb open(
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis,
            NativeDefragOptions defragOptions
    ) {
        DbDefragConfig defrag = defragOptions == null
                ? new DbDefragConfig(false, 0L, 0L, 0L)
                : new DbDefragConfig(
                        true,
                        defragOptions.maxMoveBytes(),
                        defragOptions.maxObjects(),
                        defragOptions.timeBudgetNanos() / 1_000_000L
                );
        return open(config(
                maxmemoryBytes,
                policy,
                samples,
                evictionLimitMillis,
                expireLimitMillis,
                defrag
        ));
    }

    public static YierdisDb open(
            TestBackend backend,
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis,
            NativeDefragOptions defragOptions
    ) {
        return open(backend, config(
                maxmemoryBytes,
                policy,
                samples,
                evictionLimitMillis,
                expireLimitMillis,
                defragConfig(defragOptions)
        ));
    }

    public static YierdisDb openWithNativeSlotCapacity(
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis,
            NativeDefragOptions defragOptions,
            int nativeSlotCapacity
    ) {
        return openWithFactory(
                HeapStableMemoryBackend::new,
                nativeSlotCapacity,
                config(
                        maxmemoryBytes,
                        policy,
                        samples,
                        evictionLimitMillis,
                        expireLimitMillis,
                        defragConfig(defragOptions)
                )
        );
    }

    public static YierdisDb openWithNativeSlotCapacity(
            TestBackend backend,
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis,
            NativeDefragOptions defragOptions,
            int nativeSlotCapacity
    ) {
        return openWithFactory(
                backend,
                nativeSlotCapacity,
                config(
                        maxmemoryBytes,
                        policy,
                        samples,
                        evictionLimitMillis,
                        expireLimitMillis,
                        defragConfig(defragOptions)
                )
        );
    }

    private static DbDefragConfig defragConfig(NativeDefragOptions defragOptions) {
        if (defragOptions == null) {
            return new DbDefragConfig(false, 0L, 0L, 0L);
        }
        return new DbDefragConfig(
                true,
                defragOptions.maxMoveBytes(),
                defragOptions.maxObjects(),
                defragOptions.timeBudgetNanos() / 1_000_000L
        );
    }

    public static DbEngineConfig config() {
        return new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        );
    }

    public static DbEngineConfig config(
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionLimitMillis,
            long expireLimitMillis,
            DbDefragConfig defrag
    ) {
        return new DbEngineConfig(
                0,
                maxmemoryBytes,
                policy,
                samples,
                evictionLimitMillis,
                expireLimitMillis,
                defrag
        );
    }

    static WriteResult<PoppedValueSequence> commitPop(
            ListWriteOps operations,
            byte[] keyBytes,
            int count,
            boolean left
    ) {
        PreparedMutation<PoppedValueSequence> prepared = operations.preparePop(keyBytes, count, left);
        boolean completed = false;
        try {
            PoppedValueSequence preview = prepared.preview();
            MutationOutcome outcome = prepared.commit(MutationContext.none());
            completed = true;
            return WriteResult.of(new CommittedPopSource(prepared, preview), outcome);
        } finally {
            if (!completed) {
                prepared.close();
            }
        }
    }

    static WriteResult<StringWriteOps.SetStringValue> commitSetWithOldValue(
            StringWriteOps operations,
            byte[] keyBytes,
            BytesSlice value,
            SetMode mode,
            ExpireOption expireOption
    ) {
        PreparedMutation<StringWriteOps.SetStringValue> prepared = operations.prepareSet(
                keyBytes,
                value,
                mode,
                expireOption,
                false
        );
        boolean completed = false;
        try {
            StringWriteOps.SetStringValue preview = prepared.preview();
            MutationOutcome outcome = prepared.commit(MutationContext.none());
            completed = true;
            return WriteResult.of(preview, outcome);
        } finally {
            if (!completed) {
                prepared.close();
            }
        }
    }

    private static final class CommittedPopSource implements PoppedValueSequence {
        private final PreparedMutation<PoppedValueSequence> prepared;
        private final PoppedValueSequence preview;

        private CommittedPopSource(
                PreparedMutation<PoppedValueSequence> prepared,
                PoppedValueSequence preview
        ) {
            this.prepared = prepared;
            this.preview = preview;
        }

        @Override
        public boolean isNull() {
            return preview.isNull();
        }

        @Override
        public int elementCount() {
            return preview.elementCount();
        }

        @Override
        public long retainedMemoryBytes() {
            return preview.retainedMemoryBytes();
        }

        @Override
        public void visitElementLengths(PayloadLengthSink out) {
            preview.visitElementLengths(out);
        }

        @Override
        public void emitTo(ByteValueSink out) {
            preview.emitTo(out);
        }

        @Override
        public void close() {
            prepared.close();
        }
    }
}
