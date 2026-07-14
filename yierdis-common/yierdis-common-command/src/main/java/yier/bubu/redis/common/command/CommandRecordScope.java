package yier.bubu.redis.common.command;

import java.util.Objects;

/**
 * 当前命令记录的 owner-thread 作用域。
 */
public final class CommandRecordScope {
    private static final ThreadLocal<Entry> CURRENT = new ThreadLocal<>();

    private CommandRecordScope() {
    }

    public static Scope open(ImmutableCommandRecord record) {
        Entry entry = new Entry(Objects.requireNonNull(record, "record"), CURRENT.get());
        CURRENT.set(entry);
        return new Scope(entry, Thread.currentThread());
    }

    public static ImmutableCommandRecord current() {
        Entry entry = CURRENT.get();
        return entry == null ? null : entry.record;
    }

    public static final class Scope implements AutoCloseable {
        private final Entry entry;
        private final Thread owner;
        private boolean closed;

        private Scope(Entry entry, Thread owner) {
            this.entry = entry;
            this.owner = owner;
        }

        @Override
        public void close() {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("command record scope belongs to another thread");
            }
            if (closed) {
                return;
            }
            if (CURRENT.get() != entry) {
                throw new IllegalStateException("command record scopes must close in nesting order");
            }
            closed = true;
            if (entry.previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(entry.previous);
            }
        }
    }

    private record Entry(ImmutableCommandRecord record, Entry previous) {
    }
}
