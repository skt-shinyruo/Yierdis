package yier.bubu.redis.execution.api;

import java.util.Objects;
import yier.bubu.redis.common.command.CommandRecordView;

/**
 * 命令执行记录。
 *
 * <p>公开构造函数保留独立快照语义；runtime sink 使用 {@link #borrowed(int, CommandRecordView)}
 * 交付一个受 callback 生命周期约束的只读视图。</p>
 */
public record ExecutionRecord(int dbIndex, CommandRecordView request) {
    public ExecutionRecord(int dbIndex, ExecutionRequest request) {
        this(
                Math.max(0, dbIndex),
                (CommandRecordView) ByteArrayExecutionRequest.copyOf(Objects.requireNonNull(request, "request"))
        );
    }

    public ExecutionRecord {
        dbIndex = Math.max(0, dbIndex);
        request = Objects.requireNonNull(request, "request");
    }

    public static ExecutionRecord borrowed(int dbIndex, CommandRecordView request) {
        return new ExecutionRecord(Math.max(0, dbIndex), Objects.requireNonNull(request, "request"));
    }
}
