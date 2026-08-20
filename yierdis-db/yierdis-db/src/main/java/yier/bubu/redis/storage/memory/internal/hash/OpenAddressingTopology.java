package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Objects;

/**
 * 管理 open-addressing table 的槽位元数据和 active/old 迁移生命周期。
 * key/value 数组及其所有权留给容器；matcher 和 mover 只在对应槽位仍有效的同步调用期间使用。
 */
public final class OpenAddressingTopology {
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final byte STATE_MIGRATED_SCAN_SHADOW = 3;

    private Table active;
    private Table old;
    private int rehashCursor;
    private int size;
    private long generation;
    private long completedRehashes;
    private int maximumProbeLength;

    public OpenAddressingTopology(int initialCapacity) {
        active = new Table(initialCapacity);
    }

    public ProbeResult probe(int hash, SlotMatcher matcher) {
        Objects.requireNonNull(matcher, "matcher");
        TableProbe activeProbe = probe(active, TableSide.ACTIVE, hash, matcher, true);
        if (activeProbe.foundSlot >= 0) {
            return new ProbeResult(new Location(TableSide.ACTIVE, activeProbe.foundSlot), true, activeProbe.probes);
        }

        int probes = activeProbe.probes;
        if (old != null) {
            TableProbe oldProbe = probe(old, TableSide.OLD, hash, matcher, false);
            probes = Math.max(probes, oldProbe.probes);
            if (oldProbe.foundSlot >= 0) {
                return new ProbeResult(new Location(TableSide.OLD, oldProbe.foundSlot), true, probes);
            }
        }
        if (activeProbe.insertionSlot < 0) {
            throw new IllegalStateException("active topology has no insertion slot");
        }
        return new ProbeResult(new Location(TableSide.ACTIVE, activeProbe.insertionSlot), false, probes);
    }

    public void occupyActive(int slot, int hash) {
        occupy(active, slot, hash);
        size++;
    }

    public void remove(Location location) {
        Objects.requireNonNull(location, "location");
        Table table = table(location.table());
        requireSlot(table, location.slot());
        if (table.states[location.slot()] != STATE_FILLED) {
            throw new IllegalStateException("cannot remove a non-filled topology slot");
        }
        table.states[location.slot()] = STATE_TOMBSTONE;
        table.hashes[location.slot()] = 0;
        table.tombstones++;
        size--;
    }

    public void beginRehash(int targetCapacity) {
        if (old != null) {
            throw new IllegalStateException("cannot start a second topology rehash");
        }
        old = active;
        active = new Table(targetCapacity);
        rehashCursor = 0;
        generation++;
    }

    public HashTableWorkResult advanceRehash(HashTableWorkBudget budget, SlotMover mover) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(mover, "mover");
        if (old == null) {
            return new HashTableWorkResult(0L, 0L, true, HashTableWorkResult.StopReason.NOT_REHASHING);
        }

        long inspected = 0L;
        long migrated = 0L;
        long startedAt = System.nanoTime();
        while (rehashCursor < old.capacity) {
            if (inspected >= budget.maxInspectedSlots()) {
                return new HashTableWorkResult(
                        inspected,
                        migrated,
                        false,
                        HashTableWorkResult.StopReason.SLOT_LIMIT
                );
            }
            if (timeLimitReached(startedAt, budget.timeLimitNanos())) {
                return new HashTableWorkResult(
                        inspected,
                        migrated,
                        false,
                        HashTableWorkResult.StopReason.TIME_LIMIT
                );
            }

            int oldSlot = rehashCursor++;
            inspected++;
            if (old.states[oldSlot] == STATE_FILLED) {
                moveOldSlot(oldSlot, mover);
                migrated++;
            }
        }

        old = null;
        rehashCursor = 0;
        completedRehashes++;
        generation++;
        return new HashTableWorkResult(inspected, migrated, true, HashTableWorkResult.StopReason.COMPLETE);
    }

    public int invalidateOldShadow(int hash, SlotMatcher matcher) {
        Objects.requireNonNull(matcher, "matcher");
        if (old == null) {
            return -1;
        }
        int mask = old.capacity - 1;
        int slot = hash & mask;
        for (int probes = 0; probes < old.capacity; probes++) {
            byte state = old.states[slot];
            if (state == STATE_EMPTY) {
                return -1;
            }
            Location location = new Location(TableSide.OLD, slot);
            if (state == STATE_MIGRATED_SCAN_SHADOW
                    && old.hashes[slot] == hash
                    && matcher.matches(location)) {
                old.states[slot] = STATE_TOMBSTONE;
                old.hashes[slot] = 0;
                old.filled++;
                old.tombstones++;
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return -1;
    }

    public HashTableMetrics metrics() {
        return new HashTableMetrics(
                active.capacity,
                size,
                active.filled,
                active.tombstones,
                old != null,
                old == null ? 0 : old.capacity,
                old == null ? 0 : rehashCursor,
                generation,
                completedRehashes,
                maximumProbeLength
        );
    }

    public SlotState slotState(TableSide side, int slot) {
        Table table = table(side);
        requireSlot(table, slot);
        return switch (table.states[slot]) {
            case STATE_EMPTY -> SlotState.EMPTY;
            case STATE_FILLED -> SlotState.FILLED;
            case STATE_TOMBSTONE -> SlotState.TOMBSTONE;
            case STATE_MIGRATED_SCAN_SHADOW -> SlotState.MIGRATED_SCAN_SHADOW;
            default -> throw new IllegalStateException("unknown topology slot state");
        };
    }

    private TableProbe probe(
            Table table,
            TableSide side,
            int hash,
            SlotMatcher matcher,
            boolean locateInsertion
    ) {
        int mask = table.capacity - 1;
        int slot = hash & mask;
        int firstTombstone = -1;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.states[slot];
            if (state == STATE_EMPTY) {
                int insertionSlot = firstTombstone >= 0 ? firstTombstone : slot;
                return new TableProbe(-1, locateInsertion ? insertionSlot : -1, probes);
            }
            if (state == STATE_TOMBSTONE && firstTombstone < 0) {
                firstTombstone = slot;
            } else if (state == STATE_FILLED
                    && table.hashes[slot] == hash
                    && matcher.matches(new Location(side, slot))) {
                return new TableProbe(slot, slot, probes);
            }
            slot = (slot + 1) & mask;
        }
        return new TableProbe(-1, locateInsertion ? firstTombstone : -1, table.capacity);
    }

    private void moveOldSlot(int oldSlot, SlotMover mover) {
        int hash = old.hashes[oldSlot];
        int activeSlot = insertionSlot(active, hash);
        mover.move(oldSlot, activeSlot);
        occupy(active, activeSlot, hash);
        old.states[oldSlot] = STATE_MIGRATED_SCAN_SHADOW;
        old.filled--;
    }

    private int insertionSlot(Table table, int hash) {
        int mask = table.capacity - 1;
        int slot = hash & mask;
        int firstTombstone = -1;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.states[slot];
            if (state == STATE_EMPTY) {
                return firstTombstone >= 0 ? firstTombstone : slot;
            }
            if (state == STATE_TOMBSTONE && firstTombstone < 0) {
                firstTombstone = slot;
            }
            slot = (slot + 1) & mask;
        }
        if (firstTombstone >= 0) {
            return firstTombstone;
        }
        throw new IllegalStateException("active topology has no insertion slot");
    }

    private static void occupy(Table table, int slot, int hash) {
        requireSlot(table, slot);
        byte previous = table.states[slot];
        if (previous == STATE_EMPTY) {
            table.filled++;
        } else if (previous == STATE_TOMBSTONE) {
            table.tombstones--;
        } else {
            throw new IllegalStateException("cannot occupy a live topology slot");
        }
        table.states[slot] = STATE_FILLED;
        table.hashes[slot] = hash;
    }

    private Table table(TableSide side) {
        Objects.requireNonNull(side, "side");
        if (side == TableSide.ACTIVE) {
            return active;
        }
        if (old == null) {
            throw new IllegalStateException("old topology is not available");
        }
        return old;
    }

    private void recordProbe(int probes) {
        maximumProbeLength = Math.max(maximumProbeLength, probes);
    }

    private static void requireSlot(Table table, int slot) {
        if (slot < 0 || slot >= table.capacity) {
            throw new IndexOutOfBoundsException("slot: " + slot + ", capacity: " + table.capacity);
        }
    }

    private static boolean timeLimitReached(long startedAt, long timeLimitNanos) {
        return timeLimitNanos != Long.MAX_VALUE && System.nanoTime() - startedAt >= timeLimitNanos;
    }

    private static void validateCapacity(int capacity) {
        if (capacity < HashCapacityPolicy.MIN_CAPACITY
                || capacity > HashCapacityPolicy.MAX_CAPACITY
                || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("invalid topology capacity: " + capacity);
        }
    }

    @FunctionalInterface
    public interface SlotMatcher {
        boolean matches(Location location);
    }

    @FunctionalInterface
    public interface SlotMover {
        void move(int oldSlot, int activeSlot);
    }

    public enum TableSide {
        ACTIVE,
        OLD
    }

    public enum SlotState {
        EMPTY,
        FILLED,
        TOMBSTONE,
        MIGRATED_SCAN_SHADOW
    }

    public record Location(TableSide table, int slot) {
        public Location {
            Objects.requireNonNull(table, "table");
            if (slot < 0) {
                throw new IllegalArgumentException("slot must be >= 0");
            }
        }
    }

    public record ProbeResult(Location location, boolean found, int probes) {
        public ProbeResult {
            Objects.requireNonNull(location, "location");
            if (probes <= 0) {
                throw new IllegalArgumentException("probes must be > 0");
            }
        }
    }

    private record TableProbe(int foundSlot, int insertionSlot, int probes) {
    }

    private static final class Table {
        private final int capacity;
        private final byte[] states;
        private final int[] hashes;
        private int filled;
        private int tombstones;

        private Table(int capacity) {
            validateCapacity(capacity);
            this.capacity = capacity;
            states = new byte[capacity];
            hashes = new int[capacity];
        }
    }
}
