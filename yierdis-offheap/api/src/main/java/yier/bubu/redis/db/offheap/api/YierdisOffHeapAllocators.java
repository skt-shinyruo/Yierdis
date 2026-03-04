package yier.bubu.redis.db.offheap.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

public final class YierdisOffHeapAllocators {
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

        YierdisOffHeapAllocatorProvider provider = findProvider(backend);
        if (provider == null) {
            throw new YierdisOffHeapBackendUnavailableException(
                    "Off-heap 后端 '" + backend.name().toLowerCase() + "' 在当前构建或运行环境中不可用（未发现 ServiceLoader provider）。"
                            + "请确认对应模块依赖已引入。"
                            + "已发现 providers: " + availableProvidersSummary());
        }

        try {
            return provider.create(maxBytes);
        } catch (YierdisOffHeapBackendUnavailableException e) {
            throw e;
        } catch (LinkageError e) {
            String rootCause = e.getClass().getSimpleName();
            String rootMessage = e.getMessage();
            if (rootMessage != null && !rootMessage.isBlank()) {
                rootCause = rootCause + ": " + rootMessage;
            }
            throw new YierdisOffHeapBackendUnavailableException(
                    "Off-heap 后端 '" + backend.name().toLowerCase() + "' 在当前运行环境不可用（类加载/初始化失败）。"
                            + "根因: " + rootCause + "。"
                            + "请确认对应模块依赖已引入，且运行环境满足后端要求。"
                            + "已发现 providers: " + availableProvidersSummary(),
                    e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new IllegalStateException(
                    "Failed to initialize off-heap backend via provider: " + provider.getClass().getName(),
                    t);
        }
    }

    private static YierdisOffHeapAllocatorProvider findProvider(YierdisOffHeapBackend backend) {
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
        return found;
    }
}
