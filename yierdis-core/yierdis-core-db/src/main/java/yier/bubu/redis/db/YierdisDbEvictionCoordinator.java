package yier.bubu.redis.db;

// YierdisDbEvictionCoordinator：将 YierdisDb 的淘汰/预算相关入口收敛为 EvictionCoordinator 边界。

import yier.bubu.redis.ops.EvictionCoordinator;
import yier.bubu.redis.ops.MaxmemoryCoordinator;
import yier.bubu.redis.ops.MaxmemoryCoordinatorAware;

import java.util.Objects;

final class YierdisDbEvictionCoordinator implements EvictionCoordinator, MaxmemoryCoordinatorAware {
    private final YierdisDb db;

    YierdisDbEvictionCoordinator(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public void prepareWrite(long estimatedExtraBytes) {
        db.prepareWrite(estimatedExtraBytes);
    }

    @Override
    public void enforceMaxmemory() {
        db.enforceMaxmemory();
    }

    @Override
    public void rollbackWriteReservationIfAny() {
        db.rollbackWriteReservationIfAny();
    }

    @Override
    public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        db.attachMaxmemoryCoordinator(coordinator);
    }
}
