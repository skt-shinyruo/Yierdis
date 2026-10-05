package yier.bubu.redis.memory.foreign;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;

/**
 * 按槽位编号记录当前受影响的对象。成员数就是遍历成本，弹出路径不能再借全表扫描表达同一件事。
 */
final class YierdisNativeSlotSet {
    private static final long BASE_BYTES = 64L;

    private int[] slots = new int[4];
    private int count;
    private final Map<Integer, Integer> positionBySlot = new HashMap<>();

    void add(int slotId) {
        if (slotId <= 0) {
            throw new IllegalArgumentException("slotId must be > 0");
        }
        if (positionBySlot.containsKey(slotId)) {
            throw new IllegalStateException("slot is already indexed: " + slotId);
        }
        if (count == slots.length) {
            if (slots.length > Integer.MAX_VALUE / 2) {
                throw new NativeCapacityExceededException("affected slot index is full");
            }
            slots = Arrays.copyOf(slots, slots.length << 1);
        }
        slots[count] = slotId;
        positionBySlot.put(slotId, count);
        count++;
    }

    void remove(int slotId) {
        Integer index = positionBySlot.remove(slotId);
        if (index == null) {
            throw new IllegalStateException("slot is not indexed: " + slotId);
        }
        int last = count - 1;
        int moved = slots[last];
        slots[index] = moved;
        if (index != last) {
            positionBySlot.put(moved, index);
        }
        slots[last] = 0;
        count = last;
    }

    int size() {
        return count;
    }

    int[] copy() {
        return Arrays.copyOf(slots, count);
    }

    long heapEstimatedBytes() {
        // HashMap 条目随 pin/quarantine 增减，发生在命令预留之外。快照只计这条数组，避免一次 pin 把 used bytes 顶过紧的 maxmemory。
        return BASE_BYTES + 16L + (long) slots.length * Integer.BYTES;
    }
}
