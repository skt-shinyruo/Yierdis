package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ExecutorTaskQueueTest {
    @Test
    public void globalQueuePreservesFifoOrder() {
        ExecutorTaskQueue<String, String> queue = globalQueue();

        queue.offer("a", "a1");
        queue.offer("b", "b1");
        queue.offer("a", "a2");

        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("b1", queue.poll());
        Assert.assertEquals("a2", queue.poll());
        Assert.assertNull(queue.poll());
    }

    @Test
    public void fairQueueRotatesConnectionsAndPreservesPerConnectionOrder() {
        ExecutorTaskQueue<String, String> queue = fairQueue();

        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");
        queue.offer("b", "b2");

        Assert.assertEquals(List.of("a1", "b1", "a2", "b2"),
                List.of(queue.poll(), queue.poll(), queue.poll(), queue.poll()));
        Assert.assertNull(queue.poll());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void globalBlockedHeadPreventsLaterTasksFromPassing() {
        ExecutorTaskQueue<String, String> queue = globalQueue();
        queue.offer("a", "a1");
        queue.offer("b", "b1");

        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));
        Assert.assertNull(queue.poll());
        Assert.assertTrue(queue.resumeBlocked("a", blocked));

        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("b1", queue.poll());
    }

    @Test
    public void fairBlockedHeadStopsOnlyItsConnection() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");
        queue.offer("b", "b2");

        String blocked = queue.poll();
        Assert.assertEquals("a1", blocked);
        Assert.assertTrue(queue.block("a", blocked));

        Assert.assertEquals("b1", queue.poll());
        Assert.assertEquals("b2", queue.poll());
        Assert.assertNull(queue.poll());
        Assert.assertTrue(queue.resumeBlocked("a", blocked));
        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("a2", queue.poll());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void staleRetryRunsAtHeadBeforeLaterTasks() {
        ExecutorTaskQueue<String, String> queue = globalQueue();
        queue.offer("a", "a1");
        queue.offer("b", "b1");

        String stale = queue.poll();
        Assert.assertTrue(queue.retryAtHead("a", stale));

        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("b1", queue.poll());
    }

    @Test
    public void fairStaleRetryStaysAheadOfLaterTasksForTheSameConnection() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");

        String stale = queue.poll();
        Assert.assertEquals("a1", stale);
        Assert.assertTrue(queue.retryAtHead("a", stale));

        Assert.assertEquals("b1", queue.poll());
        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("a2", queue.poll());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void cancellingGlobalBlockedHeadAllowsLaterTasksToRun() {
        ExecutorTaskQueue<String, String> queue = globalQueue();
        queue.offer("a", "a1");
        queue.offer("b", "b1");

        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));
        Assert.assertTrue(queue.cancelBlocked("a", blocked));

        Assert.assertEquals("b1", queue.poll());
        Assert.assertNull(queue.poll());
    }

    @Test
    public void cancellingFairBlockedHeadKeepsOtherAndSameConnectionOrder() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");

        String blocked = queue.poll();
        Assert.assertEquals("a1", blocked);
        Assert.assertTrue(queue.block("a", blocked));
        Assert.assertTrue(queue.cancelBlocked("a", blocked));

        Assert.assertEquals("b1", queue.poll());
        Assert.assertEquals("a2", queue.poll());
        Assert.assertNull(queue.poll());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void cancellingObservedFairBlockedHeadReschedulesLaterTasks() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");
        queue.offer("a", "a2");

        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));
        Assert.assertNull(queue.poll());
        Assert.assertTrue(queue.cancelBlocked("a", blocked));

        Assert.assertEquals("a2", queue.poll());
        Assert.assertNull(queue.poll());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void cancellingOnlyFairBlockedHeadReclaimsItsState() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");

        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));
        Assert.assertTrue(queue.cancelBlocked("a", blocked));

        Assert.assertFalse(queue.hasRunnableTasks());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void fairQueueUsesConnectionIdentityInsteadOfEquality() {
        ExecutorTaskQueue<EqualKey, String> queue = new ExecutorTaskQueue<>(SchedulingPolicy.FAIR);
        EqualKey first = new EqualKey("same");
        EqualKey second = new EqualKey("same");
        queue.offer(first, "a1");
        queue.offer(first, "a2");
        queue.offer(second, "b1");

        Assert.assertEquals(List.of("a1", "b1", "a2"),
                List.of(queue.poll(), queue.poll(), queue.poll()));
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void removingLastFairTaskReclaimsItsState() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");

        Assert.assertTrue(queue.remove("a", "a1"));

        Assert.assertFalse(queue.hasRunnableTasks());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void drainingFairQueueRecyclesBlockedAndQueuedTasksOnce() {
        ExecutorTaskQueue<String, String> queue = fairQueue();
        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");
        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));

        List<String> recycled = new ArrayList<>();
        queue.drainLeftoverTasks(recycled::add);

        Assert.assertEquals(3, recycled.size());
        Assert.assertTrue(recycled.containsAll(List.of("a1", "a2", "b1")));
        Assert.assertFalse(queue.hasRunnableTasks());
        Assert.assertEquals(0, queue.deferredFairHeads());
        Assert.assertEquals(0, queue.fairStateCount());
    }

    @Test
    public void drainingGlobalQueueContinuesAfterRecyclerFailures() {
        ExecutorTaskQueue<String, String> queue = globalQueue();
        queue.offer("a", "a1");
        queue.offer("b", "b1");
        queue.offer("c", "c1");
        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));
        List<String> attempted = new ArrayList<>();
        IllegalStateException firstFailure = new IllegalStateException("a1 cleanup failed");
        IllegalArgumentException secondFailure = new IllegalArgumentException("b1 cleanup failed");

        IllegalStateException thrown = Assert.assertThrows(IllegalStateException.class, () ->
                queue.drainLeftoverTasks(task -> {
                    attempted.add(task);
                    if (task.equals("a1")) {
                        throw firstFailure;
                    }
                    if (task.equals("b1")) {
                        throw secondFailure;
                    }
                }));

        Assert.assertSame(firstFailure, thrown);
        Assert.assertArrayEquals(new Throwable[]{secondFailure}, thrown.getSuppressed());
        Assert.assertEquals(List.of("a1", "b1", "c1"), attempted);
        Assert.assertFalse(queue.hasRunnableTasks());
        Assert.assertEquals(0, queue.deferredGlobalHeads());
    }

    private static ExecutorTaskQueue<String, String> globalQueue() {
        return new ExecutorTaskQueue<>(SchedulingPolicy.GLOBAL);
    }

    private static ExecutorTaskQueue<String, String> fairQueue() {
        return new ExecutorTaskQueue<>(SchedulingPolicy.FAIR);
    }

    private static final class EqualKey {
        private final String value;

        private EqualKey(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof EqualKey key && value.equals(key.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
