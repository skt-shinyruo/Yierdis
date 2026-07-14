package yier.bubu.redis.storage.memory.internal.value;

public interface HeapTrackedValue {
    long heapEstimatedBytes();

    void setHeapChangeListener(Runnable listener);
}
