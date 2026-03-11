package yier.bubu.redis.db.memory.api;

public interface YierdisOffHeapBuf extends yier.bubu.redis.offheap.api.OffHeapBuf {
    YierdisOffHeapSlice slice(int index, int len);
}
