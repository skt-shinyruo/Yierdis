package yier.bubu.redis.db.offheap.api;

/**
 * 表示所选 off-heap 后端在当前构建产物或运行环境中不可用。
 * <p>
 * 该异常用于区分“可预期的配置/能力缺失”与“实现 bug/未知异常”，便于上层做友好提示与稳定退出码处理。
 */
public final class YierdisOffHeapBackendUnavailableException extends IllegalArgumentException {
    public YierdisOffHeapBackendUnavailableException(String message) {
        super(message);
    }

    public YierdisOffHeapBackendUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

