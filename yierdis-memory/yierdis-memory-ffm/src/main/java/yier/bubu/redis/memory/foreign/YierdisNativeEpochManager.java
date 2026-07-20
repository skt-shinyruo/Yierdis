package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;

public final class YierdisNativeEpochManager {
    private static final NativeEpochKind[] EPOCH_KINDS = NativeEpochKind.values();

    private final ActiveEpochs[] activeEpochs = new ActiveEpochs[EPOCH_KINDS.length];

    private long currentEpoch;

    public YierdisNativeEpochManager() {
        for (NativeEpochKind kind : EPOCH_KINDS) {
            activeEpochs[kind.ordinal()] = new ActiveEpochs();
        }
    }

    public long nextEpoch() {
        return ++currentEpoch;
    }

    public NativeEpochScope begin(NativeEpochKind kind) {
        Objects.requireNonNull(kind, "kind");
        long epoch = nextEpoch();
        activeEpochs[kind.ordinal()].add(epoch);
        return new Scope(kind, epoch);
    }

    public boolean canReclaim(long retiredEpoch) {
        if (retiredEpoch <= 0) {
            return !hasActiveEpochs();
        }
        for (ActiveEpochs epochs : activeEpochs) {
            if (epochs.hasEpochAtOrBefore(retiredEpoch)) {
                return false;
            }
        }
        return true;
    }

    public long activeCount() {
        long count = 0;
        for (ActiveEpochs epochs : activeEpochs) {
            count += epochs.count();
        }
        return count;
    }

    private boolean hasActiveEpochs() {
        for (ActiveEpochs epochs : activeEpochs) {
            if (epochs.count() > 0) {
                return true;
            }
        }
        return false;
    }

    private void close(NativeEpochKind kind, long epoch) {
        activeEpochs[kind.ordinal()].remove(epoch);
    }

    private final class Scope implements NativeEpochScope {
        private final NativeEpochKind kind;
        private final long epoch;
        private boolean closed;

        private Scope(NativeEpochKind kind, long epoch) {
            this.kind = kind;
            this.epoch = epoch;
        }

        @Override
        public NativeEpochKind kind() {
            return kind;
        }

        @Override
        public long epoch() {
            return epoch;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            YierdisNativeEpochManager.this.close(kind, epoch);
        }
    }

    private static final class ActiveEpochs {
        private long[] epochs = new long[4];
        private int size;

        private void add(long epoch) {
            ensureCapacity(size + 1);
            epochs[size++] = epoch;
        }

        private void remove(long epoch) {
            for (int i = 0; i < size; i++) {
                if (epochs[i] != epoch) {
                    continue;
                }
                int tail = size - i - 1;
                if (tail > 0) {
                    System.arraycopy(epochs, i + 1, epochs, i, tail);
                }
                epochs[--size] = 0L;
                return;
            }
            throw new IllegalStateException("native epoch is not active: " + epoch);
        }

        private boolean hasEpochAtOrBefore(long epoch) {
            for (int i = 0; i < size; i++) {
                if (epochs[i] <= epoch) {
                    return true;
                }
            }
            return false;
        }

        private int count() {
            return size;
        }

        private void ensureCapacity(int required) {
            if (required <= epochs.length) {
                return;
            }
            long[] next = new long[Math.max(required, epochs.length << 1)];
            System.arraycopy(epochs, 0, next, 0, size);
            epochs = next;
        }
    }
}
