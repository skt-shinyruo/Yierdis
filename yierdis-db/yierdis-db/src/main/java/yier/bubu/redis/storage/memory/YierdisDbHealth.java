package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.storage.api.DbAccountingReconciliation;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.YierdisCommandException;

public final class YierdisDbHealth {
    public static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    private final Runnable threadChecker;
    private boolean degraded;
    private DbHealthSnapshot firstFailure;
    private DbAccountingReconciliation lastReconciliation;

    public YierdisDbHealth(Runnable threadChecker) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
    }

    public DbHealthSnapshot snapshot() {
        threadChecker.run();
        if (firstFailure == null && lastReconciliation == null) {
            return DbHealthSnapshot.healthy();
        }
        return DbHealthSnapshot.of(
                degraded,
                firstFailure == null ? null : firstFailure.failureTypeName(),
                firstFailure == null ? null : firstFailure.failureMessage(),
                firstFailure == null ? 0L : firstFailure.failureAtMillis(),
                lastReconciliation
        );
    }

    public void requireWritable() {
        threadChecker.run();
        if (degraded) {
            throw new YierdisCommandException(MISCONF_DEGRADED);
        }
    }

    public void recordInvariantFailure(Throwable failure) {
        threadChecker.run();
        Objects.requireNonNull(failure, "failure");
        degraded = true;
        if (firstFailure != null) {
            // 只保留当前 degraded episode 的首次失败；后续失败不覆盖事故起点。
            return;
        }
        firstFailure = DbHealthSnapshot.degraded(failure, System.currentTimeMillis());
    }

    public void recordReconciliation(DbAccountingReconciliation reconciliation) {
        threadChecker.run();
        lastReconciliation = Objects.requireNonNull(reconciliation, "reconciliation");
        if (reconciliation.succeeded()) {
            // 显式对账成功才结束当前 episode：失败字段随 episode 关闭，下一场事故重新入账，
            // 已解决的事故轨迹留在 lastReconciliation。
            degraded = false;
            firstFailure = null;
        }
    }
}
