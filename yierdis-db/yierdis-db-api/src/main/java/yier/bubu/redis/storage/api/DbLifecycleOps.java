package yier.bubu.redis.storage.api;

// DbLifecycleOps：DB 生命周期/管理能力边界（FLUSHDB 等）。

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;

public interface DbLifecycleOps {
    MutationOutcome flushDb();

    default DbLifecycleOps withMutationContext(MutationContext context) {
        Objects.requireNonNull(context, "context");
        return this;
    }
}
