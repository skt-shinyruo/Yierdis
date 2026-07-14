package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.integration.protocol.RespTcpTestSupport;
import yier.bubu.redis.storage.api.PostCommitMutationException;

import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplyResultUnknownTest {
    @Test
    public void postCommitFailureClosesWithoutReplacementReplyAndLeavesTheCommittedValueVisible() throws Exception {
        AtomicBoolean failAfterFirstCommand = new AtomicBoolean(true);
        YierdisServerBootstrap server = YierdisServerBootstrap.startForTests(
                delegate -> (session, request, out) -> {
                    delegate.execute(session, request, out);
                    if (failAfterFirstCommand.compareAndSet(true, false)) {
                        throw new PostCommitMutationException(
                                "injected post-commit failure",
                                new IllegalStateException("injected cause")
                        );
                    }
                },
                "--port", "0",
                "--noCleanup"
        );
        try {
            try (Socket mutating = RespTcpTestSupport.connect(server)) {
                RespTcpTestSupport.writeCommand(mutating, "SET", "result:unknown", "visible");
                RespTcpTestSupport.assertEof(mutating);
            }

            try (Socket verifier = RespTcpTestSupport.connect(server)) {
                RespTcpTestSupport.writeCommand(verifier, "GET", "result:unknown");
                Assert.assertEquals("visible", RespTcpTestSupport.bulkPayload(RespTcpTestSupport.readFrame(verifier)));
            }

            awaitOutboundCleanup(server);
            ReplyEgressStats.Snapshot stats = server.replyEgressStatsForTests().snapshot();
            Assert.assertEquals(1L, stats.resultUnknownCloses());
            Assert.assertEquals(0L, stats.activeChunks());
            Assert.assertEquals(0L, stats.activeSources());
        } finally {
            server.close();
        }
    }

    @Test
    public void disconnectDuringLargeOutputReleasesEveryReplyLease() throws Exception {
        YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--noCleanup");
        try {
            String value = RespTcpTestSupport.asciiRepeat('d', 1_024 * 1_024);
            try (Socket client = RespTcpTestSupport.connect(server)) {
                client.setReceiveBufferSize(1_024);
                RespTcpTestSupport.writeCommand(client, "SET", "disconnect:large", value);
                Assert.assertEquals("+OK\r\n", RespTcpTestSupport.readFrame(client));

                RespTcpTestSupport.writeCommand(client, "GET", "disconnect:large");
                InputStream in = client.getInputStream();
                Assert.assertEquals('$', in.read());
                client.setSoLinger(true, 0);
            }

            awaitOutboundCleanup(server);
            ReplyEgressStats.Snapshot stats = server.replyEgressStatsForTests().snapshot();
            Assert.assertEquals(0L, stats.activeChunks());
            Assert.assertEquals(0L, stats.activeSources());
        } finally {
            server.close();
        }
    }

    private static void awaitOutboundCleanup(YierdisServerBootstrap server) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
        OutboundMemoryBudgetStats outbound = null;
        ReplyEgressStats.Snapshot egress = null;
        do {
            outbound = server.outboundMemoryBudgetForTests().stats();
            egress = server.replyEgressStatsForTests().snapshot();
            if (outbound.reservedBytes() == 0L
                    && outbound.allocatedBytes() == 0L
                    && outbound.activeSlots() == 0L
                    && egress.activeChunks() == 0L
                    && egress.activeSources() == 0L) {
                return;
            }
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        Assert.fail("reply ownership did not converge: outbound=" + outbound + ", egress=" + egress);
    }
}
