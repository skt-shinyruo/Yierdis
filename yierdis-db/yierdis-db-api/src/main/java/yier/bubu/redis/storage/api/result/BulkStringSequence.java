package yier.bubu.redis.storage.api.result;

// BulkStringSequence：用于表达可流式消费的 bulk string 序列（命令层负责写头部，然后调用 emitTo）。

public interface BulkStringSequence {
    /**
     * 返回该序列将要输出的 bulk string 数量。
     * <p>
     * 命令层使用该数量写聚合类型的头部（例如数组等）。
     */
    int count();

    /**
     * 将该序列的全部 bulk string 同步写入 {@code out}。
     */
    void emitTo(BulkStringSink out);
}
