package yier.bubu.redis.storage.api;

/**
 * 单 DB 淘汰与 GLOBAL governor 共用的尝试上限。
 *
 * <p>下限为 64。key 数不超过 {@code Integer.MAX_VALUE / 2} 时取 {@code max(64, keyCount * 2)}；
 * 超过该阈值时返回 {@code Integer.MAX_VALUE}。</p>
 */
public final class MaxmemoryEvictionAttempts {
    private static final int MIN_ATTEMPTS = 64;

    private MaxmemoryEvictionAttempts() {
    }

    public static int maxAttempts(int keyCount) {
        // int 乘法在 keyCount > MAX_VALUE / 2 时回绕成负数，随后 Math.max(64, 负数) 会把上限静默降成 64。
        if (keyCount > Integer.MAX_VALUE / 2) {
            return Integer.MAX_VALUE;
        }
        return Math.max(MIN_ATTEMPTS, keyCount * 2);
    }
}
