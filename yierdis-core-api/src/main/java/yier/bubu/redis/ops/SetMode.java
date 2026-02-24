package yier.bubu.redis.ops;

// SetMode：SET 的写入模式（NORMAL/NX/XX），作为 command-facing 的稳定类型，避免泄漏具体 DB 实现。

public enum SetMode {
    NORMAL,
    NX,
    XX
}

