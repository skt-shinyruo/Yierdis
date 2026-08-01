package yier.bubu.redis.integration.protocol;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class OrderedReplyIntegrationTest {
    @Test
    public void pipelinedSmallLargeCommandErrorAndQuitRepliesKeepReceiveOrder() throws Exception {
        String value = RespTcpTestSupport.asciiRepeat('v', 8_192);
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--noCleanup"
        );
             Socket socket = RespTcpTestSupport.connect(server)) {
            RespTcpTestSupport.writeCommand(socket, "SET", "ordered:large", value);
            Assert.assertEquals("+OK\r\n", RespTcpTestSupport.readFrame(socket));

            RespTcpTestSupport.writePipeline(
                    socket,
                    new String[]{"PING"},
                    new String[]{"GET", "ordered:large"},
                    new String[]{"NO_SUCH_COMMAND"},
                    new String[]{"QUIT"}
            );

            Assert.assertEquals("+PONG\r\n", RespTcpTestSupport.readFrame(socket));
            Assert.assertEquals(value, RespTcpTestSupport.bulkPayload(RespTcpTestSupport.readFrame(socket)));
            Assert.assertEquals("-ERR unknown command 'NO_SUCH_COMMAND'\r\n", RespTcpTestSupport.readFrame(socket));
            Assert.assertEquals("+OK\r\n", RespTcpTestSupport.readFrame(socket));
            RespTcpTestSupport.assertEof(socket);
        }
    }

    @Test
    public void protocolErrorWaitsForEarlierAcceptedReplyBeforeItsTerminalReply() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--noCleanup"
        );
             Socket socket = RespTcpTestSupport.connect(server)) {
            RespTcpTestSupport.writeRaw(
                    socket,
                    RespTcpTestSupport.join(
                            RespTcpTestSupport.command("PING"),
                            "*1\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII)
                    )
            );

            Assert.assertEquals("+PONG\r\n", RespTcpTestSupport.readFrame(socket));
            String protocolError = RespTcpTestSupport.readFrame(socket);
            Assert.assertTrue(protocolError, protocolError.startsWith("-ERR Protocol error"));
            RespTcpTestSupport.assertEof(socket);
        }
    }

    @Test
    public void permanentlyOversizedRequestsUseTheOrderedErrorPath() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--noCleanup",
                "--executorQueueMaxBytes", "1"
        );
             Socket socket = RespTcpTestSupport.connect(server)) {
            RespTcpTestSupport.writePipeline(
                    socket,
                    new String[]{"PING"},
                    new String[]{"ECHO", "later"}
            );

            Assert.assertEquals("-ERR request exceeds executor queue byte limit\r\n", RespTcpTestSupport.readFrame(socket));
            Assert.assertEquals("-ERR request exceeds executor queue byte limit\r\n", RespTcpTestSupport.readFrame(socket));
        }
    }
}
