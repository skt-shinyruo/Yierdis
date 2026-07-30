package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ExecutorTaskQueueTest {
    @Test
    public void globalQueuePreservesFifoOrder() {
        ExecutorTaskQueue<String, String> queue = globalQueue();

        Assert.assertTrue(queue.offer("a", "a1"));
        Assert.assertTrue(queue.offer("b", "b1"));
        Assert.assertTrue(queue.offer("a", "a2"));

        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("b1", queue.poll());
        Assert.assertEquals("a2", queue.poll());
        Assert.assertNull(queue.poll());
    }

    @Test
    public void fairQueueRotatesConnectionsAndPreservesPerConnectionOrder() {
        FairStates states = new FairStates();
        ExecutorTaskQueue<String, String> queue = fairQueue(states);

        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");
        queue.offer("b", "b2");

        Assert.assertEquals(List.of("a1", "b1", "a2", "b2"),
                List.of(queue.poll(), queue.poll(), queue.poll(), queue.poll()));
        Assert.assertNull(queue.poll());
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
        FairStates states = new FairStates();
        ExecutorTaskQueue<String, String> queue = fairQueue(states);
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
        Assert.assertFalse(queue.hasPendingTasks());
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
        FairStates states = new FairStates();
        ExecutorTaskQueue<String, String> queue = fairQueue(states);
        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");

        String stale = queue.poll();
        Assert.assertEquals("a1", stale);
        Assert.assertTrue(queue.retryAtHead("a", stale));

        Assert.assertEquals("b1", queue.poll());
        Assert.assertEquals("a1", queue.poll());
        Assert.assertEquals("a2", queue.poll());
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
        FairStates states = new FairStates();
        ExecutorTaskQueue<String, String> queue = fairQueue(states);
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
    }

    @Test
    public void drainingFairQueueRecyclesBlockedAndQueuedTasksOnce() {
        FairStates states = new FairStates();
        ExecutorTaskQueue<String, String> queue = fairQueue(states);
        queue.offer("a", "a1");
        queue.offer("a", "a2");
        queue.offer("b", "b1");
        String blocked = queue.poll();
        Assert.assertTrue(queue.block("a", blocked));

        List<String> recycled = new ArrayList<>();
        queue.drainLeftoverTasks(recycled::add);

        Assert.assertEquals(3, recycled.size());
        Assert.assertTrue(recycled.containsAll(List.of("a1", "a2", "b1")));
        Assert.assertFalse(queue.hasPendingTasks());
        Assert.assertFalse(queue.hasRunnableTasks());
        Assert.assertEquals(0, queue.deferredFairHeads());
    }

    private static ExecutorTaskQueue<String, String> globalQueue() {
        return new ExecutorTaskQueue<>(
                SchedulingPolicy.GLOBAL,
                new ArrayBlockingQueue<>(16),
                null
        );
    }

    private static ExecutorTaskQueue<String, String> fairQueue(FairStates states) {
        return new ExecutorTaskQueue<>(SchedulingPolicy.FAIR, null, states);
    }

    private static final class FairStates implements ExecutorKeyStateProvider<String, String> {
        private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();

        @Override
        public ExecutorKeyState<String> getOrCreate(String key) {
            return states.computeIfAbsent(key, ignored -> new State());
        }
    }

    private static final class State implements ExecutorKeyState<String> {
        private final Queue<String> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scheduled = new AtomicBoolean();
        private final AtomicReference<String> blockedHead = new AtomicReference<>();
        private final AtomicBoolean blockedHeadReady = new AtomicBoolean();

        @Override
        public Queue<String> queue() {
            return queue;
        }

        @Override
        public AtomicBoolean scheduled() {
            return scheduled;
        }

        @Override
        public AtomicReference<String> blockedHead() {
            return blockedHead;
        }

        @Override
        public AtomicBoolean blockedHeadReady() {
            return blockedHeadReady;
        }
    }
}
