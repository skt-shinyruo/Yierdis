package yier.bubu.redis.storage.api.result;

// BulkStringMapPairsSupport：常用 map pairs 实现（如 empty），避免重复分配。

import java.util.Objects;

public final class BulkStringMapPairsSupport {
    private static final BulkStringMapPairs EMPTY = new BulkStringMapPairs() {
        @Override
        public int pairCount() {
            return 0;
        }

        @Override
        public void emitPairsTo(BulkStringSink out) {
            Objects.requireNonNull(out, "out");
            // 无操作
        }
    };

    private BulkStringMapPairsSupport() {
    }

    public static BulkStringMapPairs empty() {
        return EMPTY;
    }
}
