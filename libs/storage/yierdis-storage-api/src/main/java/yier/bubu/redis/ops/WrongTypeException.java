package yier.bubu.redis.ops;

// WrongTypeException：Redis 风格 WRONGTYPE 错误（用于命令层统一映射）。

public class WrongTypeException extends RuntimeException {
    public WrongTypeException() {
        super("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
