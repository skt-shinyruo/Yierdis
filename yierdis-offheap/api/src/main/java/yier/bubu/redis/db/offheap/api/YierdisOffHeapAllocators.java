package yier.bubu.redis.db.offheap.api;

import java.lang.reflect.Constructor;
import java.util.ServiceLoader;

public final class YierdisOffHeapAllocators {
    private static final String NETTY_ALLOCATOR_CLASS =
            "yier.bubu.redis.db.offheap.netty.YierdisNettyOffHeapAllocator";
    private static final String UNSAFE_ALLOCATOR_CLASS =
            "yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator";
    private static final String FOREIGN_ALLOCATOR_CLASS =
            "yier.bubu.redis.db.offheap.foreign.YierdisForeignOffHeapAllocator";

    private YierdisOffHeapAllocators() {
    }

    public static YierdisOffHeapAllocator create(String backendName, long maxBytes) {
        YierdisOffHeapBackend backend = YierdisOffHeapBackend.fromString(backendName);
        return create(backend, maxBytes);
    }

    public static YierdisOffHeapAllocator create(YierdisOffHeapBackend backend, long maxBytes) {
        if (backend == null || backend == YierdisOffHeapBackend.NONE) {
            return null;
        }

        // Prefer ServiceLoader providers for early, explicit availability checks.
        YierdisOffHeapAllocator provider = createByServiceLoader(backend, maxBytes);
        if (provider != null) {
            return provider;
        }

        switch (backend) {
            case NETTY:
                return createNetty(maxBytes);
            case UNSAFE:
                return createUnsafe(maxBytes);
            case FOREIGN:
                return createForeign(maxBytes);
            default:
                throw new IllegalArgumentException("unknown offheap backend: " + backend);
        }
    }

    private static YierdisOffHeapAllocator createByServiceLoader(YierdisOffHeapBackend backend, long maxBytes) {
        YierdisOffHeapAllocatorProvider found = null;
        for (YierdisOffHeapAllocatorProvider p : ServiceLoader.load(YierdisOffHeapAllocatorProvider.class)) {
            if (p == null) {
                continue;
            }
            YierdisOffHeapBackend b;
            try {
                b = p.backend();
            } catch (Throwable ignored) {
                continue;
            }
            if (b != backend) {
                continue;
            }
            if (found != null) {
                throw new IllegalStateException("Multiple off-heap allocator providers found for backend: " + backend);
            }
            found = p;
        }
        if (found == null) {
            return null;
        }
        try {
            return found.create(maxBytes);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to initialize off-heap backend via provider: " + backend, t);
        }
    }

    private static YierdisOffHeapAllocator createNetty(long maxBytes) {
        return createByReflection(NETTY_ALLOCATOR_CLASS, maxBytes,
                "Netty off-heap backend is not available in this build. " +
                        "Add the dependency 'yierdis-offheap-netty'.");
    }

    private static YierdisOffHeapAllocator createForeign(long maxBytes) {
        return createByReflection(FOREIGN_ALLOCATOR_CLASS, maxBytes,
                "Foreign Memory backend is not available in this build. " +
                        "Build with the Maven profile 'foreign-memory' and run with " +
                        "--add-modules jdk.incubator.foreign (Java 17).");
    }

    private static YierdisOffHeapAllocator createUnsafe(long maxBytes) {
        return createByReflection(UNSAFE_ALLOCATOR_CLASS, maxBytes,
                "Unsafe off-heap backend is not available in this build. " +
                        "Add the dependency 'yierdis-offheap-unsafe'.");
    }

    private static YierdisOffHeapAllocator createByReflection(String allocatorClass,
                                                              long maxBytes,
                                                              String missingMessage) {
        try {
            Class<?> cls = Class.forName(allocatorClass);
            Constructor<?> ctor = cls.getConstructor(long.class);
            Object instance = ctor.newInstance(maxBytes);
            return (YierdisOffHeapAllocator) instance;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(missingMessage, e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to initialize off-heap backend: " + allocatorClass, e);
        }
    }
}
