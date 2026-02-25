package yier.bubu.redis.runtime.api;

// YierdisChangeTracking：命令级“真实变更”追踪（thread-local scope）。
//
// 设计目标：
// - 事件模型仍保持最小化（YierdisChangeEvent 只携带 argv 快照，可重放）
// - emit 判定基于“本次命令是否真实改变了 Keyspace/Value/TTL 元数据”
// - 不为读命令引入额外 copy/遍历；不做昂贵的 value-equals 深比较
// - 维护任务（过期清理/淘汰）不应污染命令变更判定：无 active scope 时 mark 必须 no-op
//
// 注意：该追踪机制只负责“本次命令是否发生变更”的最小事实，不表达 key-level 细粒度差异。

import java.util.Arrays;

/**
 * Command-scope change tracking utility.
 * <p>
 * This is an internal contract shared by the command layer (to decide whether to emit change events) and the
 * DB/ops layer (to mark real changes when they happen).
 */
public final class YierdisChangeTracking {
    private YierdisChangeTracking() {
    }

    /**
     * A reusable, thread-local scope handle. Each {@link #close()} pops a single nesting level.
     * <p>
     * This intentionally narrows {@link AutoCloseable#close()} to not throw checked exceptions so that the command
     * hot path can use try-with-resources without catch/throws noise.
     */
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static final int FLAG_VALUE_CHANGED = 1;
    private static final int FLAG_TTL_CHANGED = 1 << 1;

    private static final ThreadLocal<Context> CTX = new ThreadLocal<>();

    /**
     * Begins a command-scope tracking context for the current thread.
     * <p>
     * Nested scopes are supported and are isolated: changes marked in an inner scope do NOT leak into the outer scope.
     */
    public static Scope beginScope() {
        Context ctx = CTX.get();
        if (ctx == null) {
            ctx = new Context();
            CTX.set(ctx);
        }
        ctx.begin();
        return ctx;
    }

    /**
     * Marks that the current scope has changed keyspace/value state.
     * <p>
     * No-op when no scope is active.
     */
    public static void markValueChanged() {
        Context ctx = CTX.get();
        if (ctx == null) {
            return;
        }
        ctx.mark(FLAG_VALUE_CHANGED);
    }

    /**
     * Marks that the current scope has changed TTL metadata.
     * <p>
     * No-op when no scope is active.
     */
    public static void markTtlChanged() {
        Context ctx = CTX.get();
        if (ctx == null) {
            return;
        }
        ctx.mark(FLAG_TTL_CHANGED);
    }

    public static boolean changedValue() {
        Context ctx = CTX.get();
        return ctx != null && ctx.changed(FLAG_VALUE_CHANGED);
    }

    public static boolean changedTtl() {
        Context ctx = CTX.get();
        return ctx != null && ctx.changed(FLAG_TTL_CHANGED);
    }

    public static boolean changedAny() {
        Context ctx = CTX.get();
        return ctx != null && ctx.changedAny();
    }

    private static final class Context implements Scope {
        private int depth;
        private int flags;
        private int[] stack;

        private void begin() {
            if (depth == 0) {
                depth = 1;
                flags = 0;
                return;
            }
            ensureStackCapacity(depth);
            stack[depth - 1] = flags;
            flags = 0;
            depth++;
        }

        private void ensureStackCapacity(int requiredDepth) {
            if (stack == null) {
                stack = new int[Math.max(4, requiredDepth)];
                return;
            }
            if (stack.length >= requiredDepth) {
                return;
            }
            int next = stack.length;
            while (next < requiredDepth) {
                next = next << 1;
            }
            stack = Arrays.copyOf(stack, next);
        }

        private void mark(int flag) {
            if (depth == 0) {
                return;
            }
            flags |= flag;
        }

        private boolean changed(int flag) {
            return depth > 0 && (flags & flag) != 0;
        }

        private boolean changedAny() {
            return depth > 0 && flags != 0;
        }

        @Override
        public void close() {
            if (depth == 0) {
                return;
            }
            depth--;
            if (depth == 0) {
                flags = 0;
                return;
            }
            flags = stack[depth - 1];
        }
    }
}

