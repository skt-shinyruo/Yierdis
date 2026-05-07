package yier.bubu.redis.storage.api;

// YierdisCommandException：用于将 DB/ops 层的“命令语义错误”向上抛给命令层统一映射为 reply error。

public class YierdisCommandException extends RuntimeException {
    public YierdisCommandException(String message) {
        super(message);
    }
}
