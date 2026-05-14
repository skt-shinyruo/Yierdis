package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeDefragOptions;

final class YierdisNativeDefragPlanner {
    private final NativeDefragOptions options;
    private final long startedNanos;

    private long scannedObjects;
    private long movedBytes;
    private boolean stoppedByByteBudget;
    private boolean stoppedByObjectBudget;
    private boolean stoppedByTimeBudget;

    YierdisNativeDefragPlanner(NativeDefragOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        this.startedNanos = System.nanoTime();
    }

    boolean canInspectNext() {
        if (scannedObjects >= options.maxObjects()) {
            stoppedByObjectBudget = true;
            return false;
        }
        if (elapsedNanos() >= options.timeBudgetNanos()) {
            stoppedByTimeBudget = true;
            return false;
        }
        return true;
    }

    void onCandidateInspected() {
        scannedObjects++;
    }

    boolean canMove(int bytes) {
        if (bytes > options.maxMoveBytes() - movedBytes) {
            stoppedByByteBudget = true;
            return false;
        }
        return true;
    }

    void onMoved(long bytes) {
        movedBytes += bytes;
    }

    long scannedObjects() {
        return scannedObjects;
    }

    long movedBytes() {
        return movedBytes;
    }

    boolean stoppedByByteBudget() {
        return stoppedByByteBudget;
    }

    boolean stoppedByObjectBudget() {
        return stoppedByObjectBudget;
    }

    boolean stoppedByTimeBudget() {
        return stoppedByTimeBudget;
    }

    private long elapsedNanos() {
        return System.nanoTime() - startedNanos;
    }
}
