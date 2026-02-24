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
    private static final String FOREIGN_MODULE_NAME = "jdk.incubator.foreign";

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
        } catch (YierdisOffHeapBackendUnavailableException e) {
            throw e;
        } catch (LinkageError e) {
            throw new YierdisOffHeapBackendUnavailableException(
                    "Off-heap 后端 '" + backend.name().toLowerCase() + "' 在当前运行环境不可用（类加载/初始化失败）。"
                            + "请确认对应模块依赖已引入，且运行环境满足后端要求。"
                            + "已发现 providers: " + availableProvidersSummary(),
                    e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to initialize off-heap backend via provider: " + backend, t);
        }
    }

    private static YierdisOffHeapAllocator createNetty(long maxBytes) {
        return createByReflection(NETTY_ALLOCATOR_CLASS, maxBytes, () ->
                "Netty off-heap 后端在当前构建或运行环境中不可用。请确认已引入依赖 'yierdis-offheap-netty'。"
                        + "已发现 providers: " + availableProvidersSummary());
    }

    private static YierdisOffHeapAllocator createForeign(long maxBytes) {
        // 先做构建能力探测：若构建产物未包含 foreign 模块（例如显式禁用 foreign-memory profile），这里会 ClassNotFound。
        try {
            Class.forName(FOREIGN_ALLOCATOR_CLASS, false, YierdisOffHeapAllocators.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new YierdisOffHeapBackendUnavailableException(
                    "Foreign Memory 后端在当前构建中不可用。若你从源码构建，请确认 Maven profile 'foreign-memory' 已启用（默认启用；可用 -P!foreign-memory 禁用）。"
                            + "运行时还需要添加：--add-modules " + FOREIGN_MODULE_NAME + "（Java 17）。"
                            + "已发现 providers: " + availableProvidersSummary(),
                    e);
        } catch (LinkageError e) {
            // foreign 类已存在但无法链接，通常意味着运行时未启用 incubator 模块（或 JVM 环境不支持）。
            throw new YierdisOffHeapBackendUnavailableException(
                    "Foreign Memory 后端需要在运行时启用 incubator 模块：请使用 'java --add-modules "
                            + FOREIGN_MODULE_NAME
                            + " -jar ... --offheapBackend foreign' 运行（Java 17）。",
                    e);
        }

        if (!isModulePresent(FOREIGN_MODULE_NAME)) {
            throw new YierdisOffHeapBackendUnavailableException(
                    "Foreign Memory 后端需要在运行时启用 incubator 模块：请使用 'java --add-modules "
                            + FOREIGN_MODULE_NAME
                            + " -jar ... --offheapBackend foreign' 运行（Java 17）。");
        }

        try {
            return createByReflection(FOREIGN_ALLOCATOR_CLASS, maxBytes, () ->
                    "Foreign Memory 后端初始化失败（已编译进构建，但运行环境不满足要求）。"
                            + "请确认已使用 --add-modules " + FOREIGN_MODULE_NAME + "（Java 17）。");
        } catch (LinkageError e) {
            throw new YierdisOffHeapBackendUnavailableException(
                    "Foreign Memory 后端需要在运行时启用 incubator 模块：请使用 'java --add-modules "
                            + FOREIGN_MODULE_NAME
                            + " -jar ... --offheapBackend foreign' 运行（Java 17）。",
                    e);
        }
    }

    private static YierdisOffHeapAllocator createUnsafe(long maxBytes) {
        return createByReflection(UNSAFE_ALLOCATOR_CLASS, maxBytes, () ->
                "Unsafe off-heap 后端在当前构建或运行环境中不可用。请确认已引入依赖 'yierdis-offheap-unsafe'。"
                        + "已发现 providers: " + availableProvidersSummary());
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
            throw new YierdisOffHeapBackendUnavailableException(missingMessageSupplier.get(), e);
        } catch (LinkageError e) {
            throw new YierdisOffHeapBackendUnavailableException(
                    missingMessageSupplier.get() + "（类加载/初始化失败："
                            + e.getClass().getSimpleName()
                            + (e.getMessage() == null || e.getMessage().isBlank() ? "" : (": " + e.getMessage()))
                            + "）",
                    e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to initialize off-heap backend: " + allocatorClass, e);
        }
    }

    private static boolean isModulePresent(String moduleName) {
        try {
            return ModuleLayer.boot().findModule(moduleName).isPresent();
        } catch (Throwable ignored) {
            return false;
        }
    }
}
