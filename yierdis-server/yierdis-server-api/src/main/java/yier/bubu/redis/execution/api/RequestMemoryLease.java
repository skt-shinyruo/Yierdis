package yier.bubu.redis.execution.api;

/**
 * 一个或多个请求视图共同持有的准入内存额度；每个保留视图都必须各自关闭一次。
 */
public interface RequestMemoryLease extends AutoCloseable {
    RequestMemoryLease NOOP = new NoopRequestMemoryLease();

    long reservedBytes();

    boolean released();

    RequestMemoryLease retain();

    @Override
    void close();
}

final class NoopRequestMemoryLease implements RequestMemoryLease {
    @Override
    public long reservedBytes() {
        return 0L;
    }

    @Override
    public boolean released() {
        return false;
    }

    @Override
    public RequestMemoryLease retain() {
        return this;
    }

    @Override
    public void close() {
    }
}
