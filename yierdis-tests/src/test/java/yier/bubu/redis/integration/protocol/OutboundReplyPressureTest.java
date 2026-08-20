package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.integration.protocol.RespTcpTestSupport;

import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class OutboundReplyPressureTest {
    private static final long GLOBAL_CAPACITY = 16_384L;
    private static final long CONNECTION_CAPACITY = 8_192L;
    private static final long CONTROL_RESERVATION = 2_048L;

    @Test
    public void singleReplyPreflightFailureLeavesMutationInvisibleAndReleasesItsLease() throws Exception {
        YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--noCleanup",
                "--replyGlobalCapacityBytes", "8192",
                "--replyPerConnectionCapacityBytes", "8192",
                "--replyMaxTotalBytes", "4096",
                "--replyChunkPayloadBytes", "256",
                "--replyControlReservationBytes", "2048"
        );
        try {
            String oldValue = RespTcpTestSupport.asciiRepeat('o', 2_048);
            try (Socket changing = RespTcpTestSupport.connect(server)) {
                RespTcpTestSupport.writeCommand(changing, "SET", "preflight:key", oldValue);
                Assert.assertEquals("+OK\r\n", RespTcpTestSupport.readFrame(changing));

                RespTcpTestSupport.writeCommand(changing, "SET", "preflight:key", "new", "GET");
                RespTcpTestSupport.assertEof(changing);
            }

            try (Socket verifier = RespTcpTestSupport.connect(server)) {
                RespTcpTestSupport.writeCommand(verifier, "STRLEN", "preflight:key");
                Assert.assertEquals(":" + oldValue.length() + "\r\n", RespTcpTestSupport.readFrame(verifier));
            }

            awaitNoOutboundOwnership(server.outboundMemoryBudgetForTests());
        } finally {
            server.close();
        }
    }

    @Test
    public void fairAllowsAnotherConnectionToProgressAroundAReplyCapacityBlockedHead() throws Exception {
        runBlockedHeadScenario("fair", true);
    }

    @Test
    public void globalDoesNotPassAReplyCapacityBlockedFifoHead() throws Exception {
        runBlockedHeadScenario("global", false);
    }

    @Test
    public void pipelinedExecClaimsTheConnectionBudgetBeforeFollowingPingRegisters() throws Exception {
        YierdisServerBootstrap server = YierdisServerBootstrap.start(serverArgs("fair"));
        OutboundMemoryBudget budget = server.outboundMemoryBudgetForTests();
        try (Socket client = RespTcpTestSupport.connect(server)) {
            RespTcpTestSupport.writePipeline(
                    client,
                    new String[]{"MULTI"},
                    new String[]{"PING"},
                    new String[]{"EXEC"},
                    new String[]{"PING"}
            );

            Assert.assertEquals("+OK\r\n", RespTcpTestSupport.readFrame(client));
            Assert.assertEquals("+QUEUED\r\n", RespTcpTestSupport.readFrame(client));
            Assert.assertEquals("*1\r\n+PONG\r\n", RespTcpTestSupport.readFrame(client));
            Assert.assertEquals("+PONG\r\n", RespTcpTestSupport.readFrame(client));
            awaitNoOutboundOwnership(budget);
        } finally {
            server.close();
        }
    }

    private static void runBlockedHeadScenario(String policy, boolean expectOtherConnectionProgress) throws Exception {
        YierdisServerBootstrap server = YierdisServerBootstrap.start(serverArgs(policy));
        OutboundMemoryBudget budget = server.outboundMemoryBudgetForTests();
        try (Socket blocked = RespTcpTestSupport.connect(server);
             Socket other = RespTcpTestSupport.connect(server)) {
            OutboundConnectionMemory blockedMemory = awaitConnectionMemory(server, blocked);
            OutboundMemoryLease heldConnectionCapacity = blockedMemory.reserve(5_800L, CONNECTION_CAPACITY).orElseThrow();
            try {
                RespTcpTestSupport.writeCommand(blocked, "ECHO", "blocked");
                awaitCapacityWaiter(budget);

                RespTcpTestSupport.writeCommand(other, "PING");
                if (expectOtherConnectionProgress) {
                    Assert.assertEquals("+PONG\r\n", RespTcpTestSupport.readFrame(other));
                } else {
                    other.setSoTimeout(250);
                    try {
                        RespTcpTestSupport.readFrame(other);
                        Assert.fail("GLOBAL must not pass a reply-capacity-blocked FIFO head");
                    } catch (SocketTimeoutException expected) {
                        // GLOBAL 模式下，回复容量被阻塞的队首不能让后续任务越过。
                    } finally {
                        other.setSoTimeout(5_000);
                    }
                }

                heldConnectionCapacity.close();
                Assert.assertEquals("$7\r\nblocked\r\n", RespTcpTestSupport.readFrame(blocked));
                if (!expectOtherConnectionProgress) {
                    Assert.assertEquals("+PONG\r\n", RespTcpTestSupport.readFrame(other));
                }
                awaitNoOutboundOwnership(budget);
            } finally {
                heldConnectionCapacity.close();
            }
        } finally {
            try {
                server.close();
            } finally {
                awaitNoOutboundOwnership(budget);
            }
        }
    }

    private static String[] serverArgs(String policy) {
        return new String[]{
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--noCleanup",
                "--executorSchedulingPolicy", policy,
                "--protocolMaxCommandBytes", "256",
                "--replyGlobalCapacityBytes", Long.toString(GLOBAL_CAPACITY),
                "--replyPerConnectionCapacityBytes", Long.toString(CONNECTION_CAPACITY),
                "--replyMaxTotalBytes", Long.toString(CONNECTION_CAPACITY),
                "--replyChunkPayloadBytes", "256",
                "--replyControlReservationBytes", Long.toString(CONTROL_RESERVATION)
        };
    }

    private static OutboundConnectionMemory awaitConnectionMemory(YierdisServerBootstrap server, Socket client)
            throws InterruptedException {
        int clientPort = client.getLocalPort();
        ChildChannelRegistry registry = server.childChannelRegistryForTests();
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadline) {
            for (Channel channel : registry.channelsForTests()) {
                if (!(channel.remoteAddress() instanceof InetSocketAddress remote) || remote.getPort() != clientPort) {
                    continue;
                }
                NettyExecutionConnection connection = NettyExecutionConnection.get(channel);
                if (connection != null && connection.replyGate() != null) {
                    return connection.replyGate().connectionMemoryForTests();
                }
            }
            Thread.sleep(5L);
        }
        throw new AssertionError("accepted client did not receive an outbound connection account");
    }

    private static void awaitCapacityWaiter(OutboundMemoryBudget budget) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
        while (budget.stats().waitingConnections() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        Assert.assertEquals("expected the first reply to wait for outbound capacity", 1, budget.stats().waitingConnections());
    }

    private static void awaitNoOutboundOwnership(OutboundMemoryBudget budget) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
        OutboundMemoryBudgetStats stats;
        do {
            stats = budget.stats();
            if (stats.reservedBytes() == 0L && stats.allocatedBytes() == 0L && stats.activeSlots() == 0L) {
                return;
            }
            Thread.sleep(5L);
        } while (System.nanoTime() < deadline);
        Assert.fail("outbound ownership did not converge: " + stats);
    }
}
