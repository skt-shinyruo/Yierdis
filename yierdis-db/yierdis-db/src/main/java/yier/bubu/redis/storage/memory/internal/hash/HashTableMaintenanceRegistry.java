package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Objects;

public final class HashTableMaintenanceRegistry {
    private static final long FIXED_HEAP_BYTES = 48L;
    private static final long REGISTRATION_HEAP_BYTES = 40L;

    private Registration head;
    private Registration cursor;
    private int pendingTableCount;
    private HashTableMaintenanceResult.StopReason lastStopReason = HashTableMaintenanceResult.StopReason.COMPLETE;

    public Registration registration(Participant participant) {
        return new Registration(this, Objects.requireNonNull(participant, "participant"));
    }

    public void register(Registration registration) {
        Registration current = requireOwned(registration);
        if (current.registered) {
            throw new IllegalStateException("hash-table maintenance participant is already registered");
        }
        if (head == null) {
            head = current;
            cursor = current;
            current.previous = current;
            current.next = current;
        } else {
            Registration tail = head.previous;
            current.previous = tail;
            current.next = head;
            tail.next = current;
            head.previous = current;
        }
        current.registered = true;
        pendingTableCount++;
    }

    public void unregister(Registration registration) {
        Registration current = requireOwned(registration);
        if (!current.registered) {
            return;
        }
        if (current.next == current) {
            head = null;
            cursor = null;
        } else {
            current.previous.next = current.next;
            current.next.previous = current.previous;
            if (head == current) {
                head = current.next;
            }
            if (cursor == current) {
                cursor = current.next;
            }
        }
        current.previous = null;
        current.next = null;
        current.registered = false;
        pendingTableCount--;
        if (pendingTableCount < 0) {
            throw new IllegalStateException("hash-table maintenance participant count underflow");
        }
    }

    public int pendingTableCount() {
        return pendingTableCount;
    }

    public long heapEstimatedBytes() {
        return FIXED_HEAP_BYTES + (long) pendingTableCount * REGISTRATION_HEAP_BYTES;
    }

    public HashTableMaintenanceResult.StopReason lastStopReason() {
        return lastStopReason;
    }

    public HashTableMaintenanceResult advance(HashTableWorkBudget budget) {
        return advance(budget, participant -> PreparationResult.NO_CHANGE);
    }

    public HashTableMaintenanceResult advance(HashTableWorkBudget budget, MaintenanceStarter maintenanceStarter) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(maintenanceStarter, "maintenanceStarter");
        if (pendingTableCount == 0) {
            return result(0L, 0L, HashTableMaintenanceResult.StopReason.COMPLETE);
        }

        long inspected = 0L;
        long migrated = 0L;
        long startedAt = System.nanoTime();
        while (pendingTableCount > 0) {
            if (inspected >= budget.maxInspectedSlots()) {
                return result(inspected, migrated, HashTableMaintenanceResult.StopReason.SLOT_LIMIT);
            }
            if (timeLimitReached(startedAt, budget.timeLimitNanos())) {
                return result(inspected, migrated, HashTableMaintenanceResult.StopReason.TIME_LIMIT);
            }

            Registration current = cursor == null ? head : cursor;
            if (current == null) {
                throw new IllegalStateException("hash-table maintenance registry lost its head");
            }
            cursor = current.next;
            if (!current.participant.hasMaintenanceDebt()) {
                unregister(current);
                continue;
            }

            long remainingTimeNanos = remainingTimeNanos(startedAt, budget.timeLimitNanos());
            HashTableWorkResult work = Objects.requireNonNull(
                    current.participant.advanceRehash(HashTableWorkBudget.of(1L, remainingTimeNanos)),
                    "hash-table maintenance work"
            );
            if (work.stopReason() == HashTableWorkResult.StopReason.NOT_REHASHING
                    && current.participant.hasMaintenanceDebt()) {
                PreparationResult preparation = Objects.requireNonNull(
                        maintenanceStarter.prepare(current.participant),
                        "hash-table maintenance preparation result"
                );
                if (preparation == PreparationResult.CAPACITY_LIMIT) {
                    return result(inspected, migrated, HashTableMaintenanceResult.StopReason.CAPACITY_LIMIT);
                }
                if (!current.participant.hasMaintenanceDebt()) {
                    unregister(current);
                    continue;
                }
                if (preparation != PreparationResult.STARTED) {
                    return result(inspected, migrated, HashTableMaintenanceResult.StopReason.NO_PROGRESS);
                }
                remainingTimeNanos = remainingTimeNanos(startedAt, budget.timeLimitNanos());
                work = Objects.requireNonNull(
                        current.participant.advanceRehash(HashTableWorkBudget.of(1L, remainingTimeNanos)),
                        "hash-table maintenance work after preparation"
                );
            }
            if (work.inspectedSlots() > 1L) {
                throw new IllegalStateException("hash-table participant exceeded its one-slot registry share");
            }
            inspected += work.inspectedSlots();
            migrated += work.migratedSlots();
            if (!current.participant.hasMaintenanceDebt()) {
                unregister(current);
            }
            if (work.stopReason() == HashTableWorkResult.StopReason.TIME_LIMIT) {
                return result(inspected, migrated, HashTableMaintenanceResult.StopReason.TIME_LIMIT);
            }
            if (work.inspectedSlots() == 0L && pendingTableCount > 0) {
                return result(inspected, migrated, HashTableMaintenanceResult.StopReason.NO_PROGRESS);
            }
        }
        return result(inspected, migrated, HashTableMaintenanceResult.StopReason.COMPLETE);
    }

    private HashTableMaintenanceResult result(
            long inspected,
            long migrated,
            HashTableMaintenanceResult.StopReason stopReason
    ) {
        lastStopReason = stopReason;
        return new HashTableMaintenanceResult(inspected, migrated, pendingTableCount, stopReason);
    }

    private Registration requireOwned(Registration registration) {
        Registration current = Objects.requireNonNull(registration, "registration");
        if (current.owner != this) {
            throw new IllegalArgumentException("hash-table maintenance registration belongs to another registry");
        }
        return current;
    }

    private static boolean timeLimitReached(long startedAt, long timeLimitNanos) {
        return timeLimitNanos != Long.MAX_VALUE && System.nanoTime() - startedAt >= timeLimitNanos;
    }

    private static long remainingTimeNanos(long startedAt, long timeLimitNanos) {
        if (timeLimitNanos == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long elapsed = System.nanoTime() - startedAt;
        return elapsed >= timeLimitNanos ? 0L : timeLimitNanos - elapsed;
    }

    public interface Participant {
        HashTableWorkResult advanceRehash(HashTableWorkBudget budget);

        boolean hasMaintenanceDebt();

        default long estimatedMaintenanceGrowthBytes() {
            return 0L;
        }

        default MaintenancePreparation prepareMaintenance() {
            return null;
        }
    }

    @FunctionalInterface
    public interface MaintenanceStarter {
        PreparationResult prepare(Participant participant);
    }

    public interface MaintenancePreparation {
        long stagedNonNativeGrowthBytes();

        void commit();

        void abort();
    }

    public enum PreparationResult {
        STARTED,
        NO_CHANGE,
        CAPACITY_LIMIT
    }

    public static final class Registration {
        private final HashTableMaintenanceRegistry owner;
        private final Participant participant;
        private Registration previous;
        private Registration next;
        private boolean registered;

        private Registration(HashTableMaintenanceRegistry owner, Participant participant) {
            this.owner = owner;
            this.participant = participant;
        }

        public boolean registered() {
            return registered;
        }
    }
}
