package yier.bubu.redis.storage.api;

/**
 * ExpireCondition：EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 的条件标志（NX/XX/GT/LT，Redis 7.0+），
 * 作为 command-facing 的稳定类型，避免泄漏具体 DB 实现。
 * 语义对齐 Redis expireGenericCommand：无 TTL 的键视为无限 TTL（GT 必然失败、LT 必然成功）；
 * XX 可与 GT/LT 组合成交集条件，NX 与任何其他标志互斥、GT 与 LT 互斥，互斥组合在构造时即拒绝。
 */
public record ExpireCondition(boolean nx, boolean xx, boolean gt, boolean lt) {
    public static final ExpireCondition NONE = new ExpireCondition(false, false, false, false);
    public static final ExpireCondition NX = new ExpireCondition(true, false, false, false);
    public static final ExpireCondition XX = new ExpireCondition(false, true, false, false);
    public static final ExpireCondition GT = new ExpireCondition(false, false, true, false);
    public static final ExpireCondition LT = new ExpireCondition(false, false, false, true);
    public static final ExpireCondition XX_GT = new ExpireCondition(false, true, true, false);
    public static final ExpireCondition XX_LT = new ExpireCondition(false, true, false, true);

    public ExpireCondition {
        if (nx && (xx || gt || lt)) {
            throw new IllegalArgumentException("NX is not compatible with XX, GT or LT");
        }
        if (gt && lt) {
            throw new IllegalArgumentException("GT is not compatible with LT");
        }
    }

    /**
     * 判定条件是否允许把键的过期时间改为 {@code newExpireAtMillis}。
     * {@code currentExpireAtMillis} 小于 0 表示键当前没有 TTL，按无限 TTL 参与比较。
     */
    public boolean allows(long currentExpireAtMillis, long newExpireAtMillis) {
        boolean hasTtl = currentExpireAtMillis >= 0L;
        if (nx && hasTtl) {
            return false;
        }
        if (xx && !hasTtl) {
            return false;
        }
        if (gt && (!hasTtl || newExpireAtMillis <= currentExpireAtMillis)) {
            return false;
        }
        return !lt || !hasTtl || newExpireAtMillis < currentExpireAtMillis;
    }
}
