package yier.bubu.redis.client;

/**
 * 管道里一条已经写出的命令。{@link #get()} 按发送顺序读完当时已经写出、还没读的全部回复。
 * 已经读过的句柄再取，返回保存下来的结果。服务端错误只从对应句柄抛出。
 */
public final class Reply<T> {
    private final Pipeline pipeline;
    private final Call<T> call;
    private boolean read;
    private T value;
    private RuntimeException error;

    Reply(Pipeline pipeline, Call<T> call) {
        this.pipeline = pipeline;
        this.call = call;
    }

    public T get() {
        return get(pipeline.defaultTimeoutMillis());
    }

    /**
     * {@code commandTimeoutMillis <= 0} 在读回复之前拒绝。
     * 这次调用若要连读已经写出的回复，每条都单独使用这个超时。
     */
    public T get(long commandTimeoutMillis) {
        if (commandTimeoutMillis <= 0) {
            throw new IllegalArgumentException("commandTimeoutMillis must be > 0");
        }
        if (!read) {
            pipeline.readThrough(this, commandTimeoutMillis);
        }
        return deliver();
    }

    String[] args() {
        return call.args;
    }

    boolean isRead() {
        return read;
    }

    void complete(Object raw) {
        value = call.decode(raw);
        read = true;
    }

    void fail(RuntimeException failure) {
        error = failure;
        read = true;
    }

    private T deliver() {
        if (error != null) {
            throw error;
        }
        return value;
    }
}
