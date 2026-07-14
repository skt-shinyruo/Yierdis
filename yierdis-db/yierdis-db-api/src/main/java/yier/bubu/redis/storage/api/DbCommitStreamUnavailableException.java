package yier.bubu.redis.storage.api;

/**
 * 写入在 storage 可见性之前未能取得 commit stream 容量。
 */
public final class DbCommitStreamUnavailableException extends YierdisCommandException {
    public DbCommitStreamUnavailableException() {
        super("BUSY commit stream unavailable");
    }
}
