package yier.bubu.redis.command.api;

// SlowCommandGovernor：慢命令治理接口（时间预算/输出上限/可观测），优先用于 KEYS/SCAN 等潜在长耗时命令。

import yier.bubu.redis.contract.CommandContext;

/**
 * Slow command governance (minimal contract).
 * <p>
 * 设计目标：
 * <ul>
 *   <li>为潜在慢命令提供可配置的“时间预算/输出上限”</li>
 *   <li>默认策略保守但不破坏正常小数据集的语义</li>
 *   <li>实现侧应尽量在预算耗尽前 fail-fast，以避免阻塞 executor</li>
 * </ul>
 */
public interface SlowCommandGovernor {
    SlowCommandGovernor UNBOUNDED = new SlowCommandGovernor() {
        @Override
        public long keysTimeBudgetNanos(CommandContext ctx) {
            return 0;
        }
    };

    SlowCommandGovernor DEFAULT = new SlowCommandGovernor() {
        @Override
        public long keysTimeBudgetNanos(CommandContext ctx) {
            // 约定：KEYS 本质是全表扫描，默认加一个温和预算以避免极端情况下阻塞 executor。
            // 小数据集下几乎不会触发，且 COUNT/分页场景建议使用 SCAN。
            return java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(20);
        }
    };

    /**
     * Time budget for KEYS command (0 means unlimited).
     */
    long keysTimeBudgetNanos(CommandContext ctx);

    /**
     * Max results for KEYS (best-effort). When exceeded, implementations should fail-fast.
     * <p>
     * Default is "no limit" (preserve Redis-compatible behavior).
     */
    default int keysMaxResults(CommandContext ctx) {
        return Integer.MAX_VALUE;
    }
}
