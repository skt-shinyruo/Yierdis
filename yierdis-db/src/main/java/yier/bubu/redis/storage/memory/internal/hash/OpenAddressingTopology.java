package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Objects;

/**
 * 管理 open-addressing table 的槽位元数据和 active/old 迁移生命周期。
 * key/value 数组及其所有权留给容器；matcher 和 mover 只在对应槽位仍有效的同步调用期间使用。
 */
public final class OpenAddressingTopology {
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long TOPOLOGY_OBJECT_BYTES = 64L;
    private static final long TABLE_OBJECT_BYTES = 48L;
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
        beginRehash(new OpenAddressingTopology(targetCapacity));
    }

    /** 发布预先分配的空 active topology，并把当前 active 转为 old。 */
    public void beginRehash(OpenAddressingTopology replacement) {
        Objects.requireNonNull(replacement, "replacement");
        if (old != null) {
            throw new IllegalStateException("cannot start a second topology rehash");
        }
        if (replacement == this
                || replacement.old != null
                || replacement.size != 0
                || replacement.active.filled != 0
                || replacement.active.tombstones != 0) {
            throw new IllegalArgumentException("replacement must be an empty standalone active topology");
        }
        old = active;
        active = replacement.active;
        rehashCursor = 0;
        generation++;
    }

    /** 将指定 old slot 同步提升到 active，并返回 mover 收到的 active slot。 */
    public int promoteOld(int oldSlot, SlotMover mover) {
        Objects.requireNonNull(mover, "mover");
        if (old == null) {
            throw new IllegalStateException("old topology is not available");
        }
        requireSlot(old, oldSlot);
        if (old.states[oldSlot] != STATE_FILLED) {
            throw new IllegalStateException("cannot promote a non-filled old slot");
        }
        return moveOldSlot(oldSlot, mover);
    }

    /** 发布一个已完整构建的 standalone topology，同时保留本实例的生命周期计数。 */
    public void replaceActive(OpenAddressingTopology replacement) {
        Objects.requireNonNull(replacement, "replacement");
        if (replacement == this || replacement.old != null) {
            throw new IllegalArgumentException("replacement must be a standalone active topology");
        }
        boolean replacedRehash = old != null;
        active = replacement.active;
        old = null;
        rehashCursor = 0;
        size = replacement.size;
        maximumProbeLength = Math.max(maximumProbeLength, replacement.maximumProbeLength);
        if (replacedRehash) {
            completedRehashes++;
        }
        generation++;
    }

    /** 清空 active/old 状态，但保留跨 clear 的历史计数。 */
    public void reset(int initialCapacity) {
        reset(new OpenAddressingTopology(initialCapacity));
    }

    /** 发布预先分配的空 active topology，并保留跨 clear 的历史计数。 */
    public void reset(OpenAddressingTopology replacement) {
        Objects.requireNonNull(replacement, "replacement");
        if (replacement == this
                || replacement.old != null
                || replacement.size != 0
                || replacement.active.filled != 0
                || replacement.active.tombstones != 0) {
            throw new IllegalArgumentException("replacement must be an empty standalone active topology");
        }
        active = replacement.active;
        old = null;
        rehashCursor = 0;
        size = 0;
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
                promoteOld(oldSlot, mover);
                migrated++;
            }
        }

        old = null;
        rehashCursor = 0;
        completedRehashes++;
        generation++;
        return new HashTableWorkResult(inspected, migrated, true, HashTableWorkResult.StopReason.COMPLETE);
    }

    /**
     * 按 Redis {@code dictScan} 同族的反向二进制游标推进一次 SCAN。
     * <p>
     * 游标是哈希空间中的本位槽位置（非某代表的物理槽）；扩容、缩容或 rehash 完成后仍可映射到新表继续扫。
     * 契约：全程存在的元素至少交出一次；只扩容时不重复，缩容时可能重复。visitor 返回 false 只会让
     * 本次调用在当前游标位置扫完后停下（游标无法表示“本位槽扫了一半”）。visitor 不得修改拓扑。
     *
     * @param cursor 客户端游标；超出 32 位的值无法映射到哈希空间，按从头开始处理
     */
    public ScanStep scan(long cursor, long maxInspectedSlots, ScanVisitor visitor) {
        Objects.requireNonNull(visitor, "visitor");
        if (maxInspectedSlots < 0L) {
            throw new IllegalArgumentException("maxInspectedSlots must be >= 0");
        }
        // 双表期：小表一次本位槽，大表扫完与之低位相同的全部本位槽（与 Redis dictScan 同位覆盖一致）。
        TableSide smallSide = TableSide.ACTIVE;
        Table small = active;
        TableSide largeSide = null;
        Table large = null;
        if (old != null) {
            boolean oldIsSmaller = old.capacity < active.capacity;
            smallSide = oldIsSmaller ? TableSide.OLD : TableSide.ACTIVE;
            small = oldIsSmaller ? old : active;
            largeSide = oldIsSmaller ? TableSide.ACTIVE : TableSide.OLD;
            large = oldIsSmaller ? active : old;
        }
        int smallMask = small.capacity - 1;
        int largeMask = large == null ? smallMask : large.capacity - 1;
        int start = cursor < 0L || cursor > 0xFFFF_FFFFL ? 0 : (int) cursor & largeMask;
        if (size == 0) {
            return new ScanStep(start, 0L, 0L);
        }

        ScanRun run = new ScanRun(visitor);
        int position = start;
        while (run.inspected < maxInspectedSlots && !run.stopRequested) {
            // 线性探测：本位 b 的 FILLED 都落在从 b 到第一个 EMPTY 的连续区间；只交出 home 匹配的 FILLED。
            run.visitHomeSlot(smallSide, small, position & smallMask);
            if (large == null) {
                position = reverseIncrement(position, smallMask);
            } else {
                do {
                    run.visitHomeSlot(largeSide, large, position & largeMask);
                    position = reverseIncrement(position, largeMask);
                    // 大表 mask 多出的高位清零前，继续扫同一小表桶对应的大表同位集合。
                } while ((position & (smallMask ^ largeMask)) != 0);
            }
            if (position == 0) {
                return new ScanStep(start, 0L, run.inspected);
            }
        }
        return new ScanStep(start, position, run.inspected);
    }

    /**
     * Redis {@code dictScan} 同族的反向二进制步进：在 {@code mask} 覆盖的低位空间里，
     * 按“位反转后的自然序”前进一格。扩容后新表多出的高位桶会排在旧游标之后，因此已扫过的
     * 前缀不必重扫；缩容时部分高位塌缩，允许重复。
     */
    private static int reverseIncrement(int position, int mask) {
        return Integer.reverse(Integer.reverse(position | ~mask) + 1);
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

    public long heapEstimatedBytes() {
        return TOPOLOGY_OBJECT_BYTES
                + tableHeapEstimatedBytes(active.capacity)
                + (old == null ? 0L : tableHeapEstimatedBytes(old.capacity));
    }

    public static long standaloneHeapEstimatedBytes(int capacity) {
        validateCapacity(capacity);
        return TOPOLOGY_OBJECT_BYTES + tableHeapEstimatedBytes(capacity);
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

    /** 返回 FILLED 或 MIGRATED_SCAN_SHADOW slot 保留的 probe hash。 */
    public int hashAt(Location location) {
        Objects.requireNonNull(location, "location");
        Table table = table(location.table());
        requireSlot(table, location.slot());
        byte state = table.states[location.slot()];
        if (state != STATE_FILLED && state != STATE_MIGRATED_SCAN_SHADOW) {
            throw new IllegalStateException("topology slot does not retain a hash");
        }
        return table.hashes[location.slot()];
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

    private int moveOldSlot(int oldSlot, SlotMover mover) {
        int hash = old.hashes[oldSlot];
        int activeSlot = insertionSlot(active, hash);
        mover.move(oldSlot, activeSlot);
        occupy(active, activeSlot, hash);
        old.states[oldSlot] = STATE_MIGRATED_SCAN_SHADOW;
        old.filled--;
        return activeSlot;
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

    private static long tableHeapEstimatedBytes(int capacity) {
        long hashes = ARRAY_HEADER_BYTES + (long) capacity * Integer.BYTES;
        long states = ARRAY_HEADER_BYTES + capacity;
        return TABLE_OBJECT_BYTES + hashes + states;
    }

    @FunctionalInterface
    public interface SlotMatcher {
        boolean matches(Location location);
    }

    @FunctionalInterface
    public interface SlotMover {
        void move(int oldSlot, int activeSlot);
    }

    @FunctionalInterface
    public interface ScanVisitor {
        /** 返回 false 表示本次 scan 在当前游标位置扫完后停止。 */
        boolean visit(Location location);
    }

    /** 游标都是非负值；{@code nextCursor == 0} 表示完整迭代结束。 */
    public record ScanStep(long startCursor, long nextCursor, long inspectedSlots) {
    }

    private static final class ScanRun {
        private final ScanVisitor visitor;
        private long inspected;
        private boolean stopRequested;

        private ScanRun(ScanVisitor visitor) {
            this.visitor = visitor;
        }

        private void visitHomeSlot(TableSide side, Table table, int home) {
            // COUNT / maxInspectedSlots 按“本位槽”计费，对齐 Redis 对 hash bucket 的计数；
            // 线性探测链上的 EMPTY/TOMBSTONE/错位槽只是扫完该本位槽的内部工作。
            inspected++;
            int mask = table.capacity - 1;
            int slot = home;
            for (int probes = 0; probes < table.capacity; probes++) {
                byte state = table.states[slot];
                if (state == STATE_EMPTY) {
                    return;
                }
                if (state == STATE_FILLED
                        && (table.hashes[slot] & mask) == home
                        && !visitor.visit(new Location(side, slot))) {
                    stopRequested = true;
                }
                slot = (slot + 1) & mask;
            }
        }
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
