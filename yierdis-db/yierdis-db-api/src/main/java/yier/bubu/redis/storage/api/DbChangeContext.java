package yier.bubu.redis.storage.api;

public final class DbChangeContext {
    private static final ThreadLocal<DbChangeListener> CURRENT = new ThreadLocal<>();

    private DbChangeContext() {
    }

    public static Scope open(DbChangeListener listener) {
        DbChangeListener previous = CURRENT.get();
        CURRENT.set(listener == null ? DbChangeListener.NOOP : listener);
        return new Scope(previous);
    }

    public static void emit(DbChange change) {
        DbChangeListener listener = CURRENT.get();
        if (listener == null || listener == DbChangeListener.NOOP || change == null) {
            return;
        }
        listener.onDbChange(change);
    }

    public static final class Scope implements AutoCloseable {
        private final DbChangeListener previous;
        private boolean closed;

        private Scope(DbChangeListener previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }
}
