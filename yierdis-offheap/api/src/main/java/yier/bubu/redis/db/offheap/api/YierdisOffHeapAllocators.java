package yier.bubu.redis.db.offheap.api;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

public final class YierdisOffHeapAllocators {
    private static final String NETTY_ALLOCATOR_CLASS =
            "yier.bubu.redis.db.offheap.netty.YierdisNettyOffHeapAllocator";
    private static final String UNSAFE_ALLOCATOR_CLASS =
            "yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator";
    private static final String FOREIGN_ALLOCATOR_CLASS =
            "yier.bubu.redis.db.offheap.foreign.YierdisForeignOffHeapAllocator";

    private YierdisOffHeapAllocators() {
    }

    public static final class ProviderInfo {
        private final YierdisOffHeapBackend backend;
        private final String providerClassName;

        private ProviderInfo(YierdisOffHeapBackend backend, String providerClassName) {
            this.backend = backend;
            this.providerClassName = providerClassName;
        }

        public YierdisOffHeapBackend backend() {
            return backend;
        }

        public String providerClassName() {
            return providerClassName;
        }
    }

    /**
     * 返回通过 ServiceLoader 发现的 providers（用于诊断/日志）。
     * <p>
     * 该方法为 best-effort：会忽略异常/损坏的 provider，而不是 fail-fast，便于在启动诊断路径中安全调用。
     */
    public static List<ProviderInfo> availableProviders() {
        List<ProviderInfo> out = new ArrayList<>();
        try {
            for (YierdisOffHeapAllocatorProvider p : ServiceLoader.load(YierdisOffHeapAllocatorProvider.class)) {
                if (p == null) {
                    continue;
                }
                YierdisOffHeapBackend b;
                try {
                    b = p.backend();
                } catch (Throwable ignored) {
                    b = null;
                }
                String name;
                try {
                    name = p.getClass().getName();
                } catch (Throwable ignored) {
                    name = "<unknown>";
                }
                out.add(new ProviderInfo(b, name));
            }
        } catch (Throwable ignored) {
            // 忽略：诊断路径不应阻塞启动流程
        }
        return Collections.unmodifiableList(out);
    }

    public static String availableProvidersSummary() {
        List<ProviderInfo> providers = availableProviders();
        if (providers.isEmpty()) {
            return "<none>";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < providers.size(); i++) {
            ProviderInfo p = providers.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(p.backend == null ? "?" : p.backend.name());
            sb.append(":");
            sb.append(p.providerClassName == null ? "<unknown>" : p.providerClassName);
        }
        return sb.toString();
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
        return createByReflection(NETTY_ALLOCATOR_CLASS, maxBytes, () ->
                "Netty off-heap backend is not available in this build. " +
                        "Add the dependency 'yierdis-offheap-netty'. " +
                        "Discovered providers: " + availableProvidersSummary());
    }

    private static YierdisOffHeapAllocator createForeign(long maxBytes) {
        return createByReflection(FOREIGN_ALLOCATOR_CLASS, maxBytes, () ->
                "Foreign Memory backend is not available in this build. " +
                        "Build with the Maven profile 'foreign-memory' and run with " +
                        "--add-modules jdk.incubator.foreign (Java 17). " +
                        "Discovered providers: " + availableProvidersSummary());
    }

    private static YierdisOffHeapAllocator createUnsafe(long maxBytes) {
        return createByReflection(UNSAFE_ALLOCATOR_CLASS, maxBytes, () ->
                "Unsafe off-heap backend is not available in this build. " +
                        "Add the dependency 'yierdis-offheap-unsafe'. " +
                        "Discovered providers: " + availableProvidersSummary());
    }

    private static YierdisOffHeapAllocator createByReflection(String allocatorClass,
                                                              long maxBytes,
                                                              Supplier<String> missingMessageSupplier) {
        try {
            Class<?> cls = Class.forName(allocatorClass);
            Constructor<?> ctor = cls.getConstructor(long.class);
            Object instance = ctor.newInstance(maxBytes);
            return (YierdisOffHeapAllocator) instance;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(missingMessageSupplier.get(), e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to initialize off-heap backend: " + allocatorClass, e);
        }
    }
}
