package yier.bubu.redis.storage.memory.internal.hash;

import org.junit.Assert;
import org.junit.Test;

public class HashTableMaintenanceRegistryTest {
    @Test
    public void rotatesRegisteredParticipantsWithinTheExactSlotBudget() {
        HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
        TestParticipant first = new TestParticipant(8);
        TestParticipant second = new TestParticipant(8);
        registry.register(registry.registration(first));
        registry.register(registry.registration(second));

        HashTableMaintenanceResult firstTick = registry.advance(HashTableWorkBudget.of(3, Long.MAX_VALUE));

        Assert.assertEquals(3L, firstTick.inspectedSlots());
        Assert.assertEquals(2, first.calls());
        Assert.assertEquals(1, second.calls());
        Assert.assertEquals(HashTableMaintenanceResult.StopReason.SLOT_LIMIT, firstTick.stopReason());
        Assert.assertEquals(2, registry.pendingTableCount());

        HashTableMaintenanceResult secondTick = registry.advance(HashTableWorkBudget.of(1, Long.MAX_VALUE));

        Assert.assertEquals(1L, secondTick.inspectedSlots());
        Assert.assertEquals(2, second.calls());
        Assert.assertEquals(2, first.calls());
    }

    @Test
    public void unregistersCompletedAndExplicitlyClosedParticipantsWithoutWalkingIdleTables() {
        HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
        long idleHeapBytes = registry.heapEstimatedBytes();
        TestParticipant completed = new TestParticipant(1);
        TestParticipant idle = new TestParticipant(0);
        HashTableMaintenanceRegistry.Registration completedRegistration = registry.registration(completed);
        HashTableMaintenanceRegistry.Registration idleRegistration = registry.registration(idle);

        registry.register(completedRegistration);
        HashTableMaintenanceResult result = registry.advance(HashTableWorkBudget.of(4, Long.MAX_VALUE));

        Assert.assertEquals(1L, result.inspectedSlots());
        Assert.assertEquals(0, registry.pendingTableCount());
        Assert.assertEquals(idleHeapBytes, registry.heapEstimatedBytes());
        Assert.assertEquals(0, idle.calls());

        registry.register(idleRegistration);
        registry.unregister(idleRegistration);

        Assert.assertEquals(0, registry.pendingTableCount());
        Assert.assertEquals(idleHeapBytes, registry.heapEstimatedBytes());
    }

    @Test(expected = IllegalStateException.class)
    public void rejectsDuplicateRegistration() {
        HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
        HashTableMaintenanceRegistry.Registration registration = registry.registration(new TestParticipant(1));
        registry.register(registration);
        registry.register(registration);
    }

    private static final class TestParticipant implements HashTableMaintenanceRegistry.Participant {
        private int remainingSlots;
        private int calls;

        private TestParticipant(int remainingSlots) {
            this.remainingSlots = remainingSlots;
        }

        @Override
        public HashTableWorkResult advanceRehash(HashTableWorkBudget budget) {
            calls++;
            if (remainingSlots == 0) {
                return new HashTableWorkResult(0L, 0L, true, HashTableWorkResult.StopReason.NOT_REHASHING);
            }
            if (budget.maxInspectedSlots() == 0L) {
                return new HashTableWorkResult(0L, 0L, false, HashTableWorkResult.StopReason.SLOT_LIMIT);
            }
            remainingSlots--;
            return new HashTableWorkResult(
                    1L,
                    0L,
                    remainingSlots == 0,
                    remainingSlots == 0
                            ? HashTableWorkResult.StopReason.COMPLETE
                            : HashTableWorkResult.StopReason.SLOT_LIMIT
            );
        }

        @Override
        public boolean hasMaintenanceDebt() {
            return remainingSlots > 0;
        }

        private int calls() {
            return calls;
        }
    }
}
