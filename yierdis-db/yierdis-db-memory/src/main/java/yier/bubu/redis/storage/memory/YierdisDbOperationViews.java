package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;

record YierdisDbOperationViews(DbReads reads, DbWrites writes, DbLifecycleOps lifecycle) {
    YierdisDbOperationViews {
        Objects.requireNonNull(reads, "reads");
        Objects.requireNonNull(writes, "writes");
        Objects.requireNonNull(lifecycle, "lifecycle");
    }
}
