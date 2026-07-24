package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

/** 为子包测试组装生产 mutation executor，不向生产 API 增加兼容构造器。 */
public final class MutationExecutorTestSupport {
    private MutationExecutorTestSupport() {
    }

    public static YierdisDbMutationExecutor create(YierdisDb db) {
        return new YierdisDbMutationExecutor(
                db::checkThread,
                db.memoryLedger(),
                db.stableMemoryBackend(),
                db.healthMonitor(),
                db::commitPublisher,
                db::commitDbIndex
        );
    }
}
