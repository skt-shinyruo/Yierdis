package yier.bubu.redis;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.TransactionState;

import java.util.Arrays;

public class ServerConnectionContextTest {
    @Test
    public void getOrCreateReturnsSameContextForSameChannel() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext c1 = ServerConnectionContext.getOrCreate(ch);
            ServerConnectionContext c2 = ServerConnectionContext.getOrCreate(ch);

            Assert.assertSame(c1, c2);
            Assert.assertSame(c1.commandSession(), c2.commandSession());
            Assert.assertSame(c1.queueState(), c2.queueState());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void firstInitializationConfiguresTransactionQueueLimits() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext ctx = ServerConnectionContext.getOrCreate(ch, 1, 0);
            TransactionState tx = ctx.commandSession().transaction();
            tx.begin();

            Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(request("GET", "k")));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void existingContextIsNotReconfiguredByLaterGetOrCreateCalls() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext.getOrCreate(ch, 1, 0);
            ServerConnectionContext.getOrCreate(ch, 4, 0);

            TransactionState tx = ServerConnectionContext.getOrCreate(ch).commandSession().transaction();
            tx.begin();
            Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(request("PING")));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void queueStateAccessDoesNotPreventLaterSessionLimitConfiguration() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);

            Assert.assertNotNull(context.queueState());

            ServerConnectionContext.getOrCreate(ch, 1, 0);

            TransactionState tx = context.commandSession().transaction();
            tx.begin();
            Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(request("PING")));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void oversizedRequestIsRejectedBeforeSnapshotAllocation() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch, 16, 1);
            TransactionState tx = context.commandSession().transaction();
            tx.begin();

            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(new OversizedExecutionRequest()));
            Assert.assertTrue(tx.aborted());
            Assert.assertEquals(0, tx.size());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void markClosingIsIdempotentAndDiscardsTransaction() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch, 16, 1024);
            TransactionState tx = context.commandSession().transaction();
            tx.begin();
            Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
            Assert.assertTrue(tx.active());

            Assert.assertTrue(context.markClosing());
            Assert.assertFalse(context.markClosing());
            Assert.assertFalse(context.commandSession().transaction().active());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static ExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], Arrays.asList(Arrays.copyOfRange(args, 1, args.length)));
    }

    private static final class OversizedExecutionRequest implements ExecutionRequest {
        @Override
        public int argc() {
            return 1;
        }

        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int len(int index) {
            return 4;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return "PING".getBytes()[offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            throw new AssertionError("snapshot copy should not happen before queue byte guard");
        }

        @Override
        public byte[] toByteArray(int index) {
            throw new AssertionError("snapshot copy should not happen before queue byte guard");
        }

        @Override
        public int retainedBytes() {
            return 4;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
