package yier.bubu.redis.db.offheap.api;

public interface YierdisDirectBytesSink extends YierdisBytesSink {
    void ensureWritable(int len);

    int writerIndex();

    void writerIndex(int writerIndex);

    boolean hasMemoryAddress();

    long memoryAddress();
}

