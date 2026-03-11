package yier.bubu.redis.testutil;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.memory.unsafe.YierdisUnsafeOffHeapAllocator;

import java.util.Objects;
import java.util.function.Consumer;

public final class TestDbs {
    private TestDbs() {
    }

    public static void forEachDb(Consumer<YierdisDb> test) {
        Objects.requireNonNull(test, "test");
        runHeap(test);
        runUnsafeOffHeap(test);
    }

    public static void runHeap(Consumer<YierdisDb> test) {
        Objects.requireNonNull(test, "test");
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            test.accept(db);
        } finally {
            db.shutdown();
        }
    }

    public static void runUnsafeOffHeap(Consumer<YierdisDb> test) {
        Objects.requireNonNull(test, "test");
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            db.bindToCurrentThread();
            test.accept(db);
        } finally {
            db.shutdown();
        }
    }

    public static void forEachDbWithMaxmemory(long maxmemoryBytes, String maxmemoryPolicy, int maxmemorySamples, Consumer<YierdisDb> test) {
        Objects.requireNonNull(test, "test");
        YierdisDb heap = new YierdisDb(null, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, 5, 5);
        try {
            heap.bindToCurrentThread();
            test.accept(heap);
        } finally {
            heap.shutdown();
        }

        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb offHeap = new YierdisDb(allocator, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, 5, 5);
        try {
            offHeap.bindToCurrentThread();
            test.accept(offHeap);
        } finally {
            offHeap.shutdown();
        }
    }
}
