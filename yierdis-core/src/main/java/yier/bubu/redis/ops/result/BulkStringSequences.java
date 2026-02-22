package yier.bubu.redis.ops.result;

// BulkStringSequences：常用序列实现（如 empty），避免重复分配。

import java.util.Objects;

public final class BulkStringSequences {
    private static final BulkStringSequence EMPTY = new BulkStringSequence() {
        @Override
        public int count() {
            return 0;
        }

        @Override
        public void emitTo(BulkStringSink out) {
            Objects.requireNonNull(out, "out");
            // 无操作
        }
    };

    private BulkStringSequences() {
    }

    public static BulkStringSequence empty() {
        return EMPTY;
    }
}
