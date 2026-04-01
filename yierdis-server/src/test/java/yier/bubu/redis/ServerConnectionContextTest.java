package yier.bubu.redis;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.TransactionState;

import java.nio.charset.StandardCharsets;

public class ServerConnectionContextTest {
    @Test
    public void getOrCreateReturnsSameContextForSameChannel() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext c1 = ServerConnectionContext.getOrCreate(ch);
            ServerConnectionContext c2 = ServerConnectionContext.getOrCreate(ch);

            Assert.assertSame(c1, c2);
            Assert.assertSame(c1.session(), c2.session());
            Assert.assertSame(c1.runtime(), c2.runtime());
            Assert.assertSame(c1.scheduling(), c2.scheduling());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void firstInitializationConfiguresTransactionQueueLimits() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext ctx = ServerConnectionContext.getOrCreate(ch, 1, 0);
            TransactionState tx = ctx.session().transaction();
            tx.begin();

            Assert.assertNull(tx.tryEnqueue(argv("SET", "k", "v")));
            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(argv("GET", "k")));
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

            TransactionState tx = ServerConnectionContext.getOrCreate(ch).session().transaction();
            tx.begin();
            Assert.assertNull(tx.tryEnqueue(argv("SET", "k", "v")));
            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(argv("PING")));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void runtimeAndSchedulingAccessDoNotPreventLaterSessionLimitConfiguration() {
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);

            Assert.assertNotNull(context.runtime());
            Assert.assertNotNull(context.scheduling());

            ServerConnectionContext.getOrCreate(ch, 1, 0);

            TransactionState tx = context.session().transaction();
            tx.begin();
            Assert.assertNull(tx.tryEnqueue(argv("SET", "k", "v")));
            Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(argv("PING")));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static byte[][] argv(String... args) {
        byte[][] out = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            out[i] = args[i].getBytes(StandardCharsets.US_ASCII);
        }
        return out;
    }
}
