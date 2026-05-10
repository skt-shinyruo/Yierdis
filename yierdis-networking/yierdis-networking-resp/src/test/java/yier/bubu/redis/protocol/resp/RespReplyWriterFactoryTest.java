package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.execution.api.TransactionState;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespReplyWriterFactoryTest {
    @Test
    public void usesSessionRespVersionWhenAvailable() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriterFactory factory = new RespReplyWriterFactory();

        ReplyWriter writer = factory.newWriter(serverSession(3), sink);
        writer.booleanValue(true);

        Assert.assertEquals("#t\r\n", sink.utf8());
    }

    @Test
    public void sessionBackedWriterObservesProtocolChangesBeforeReplyIsWritten() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriterFactory factory = new RespReplyWriterFactory();
        MutableSession session = new MutableSession(2);

        ReplyWriter writer = factory.newWriter(session, sink);
        session.setRespVersion(3);
        writer.mapHeader(1);
        writer.bulkString("proto".getBytes(StandardCharsets.US_ASCII));
        writer.integer(3);

        Assert.assertEquals("%1\r\n$5\r\nproto\r\n:3\r\n", sink.utf8());
    }

    @Test
    public void fallsBackToResp2WhenSessionIsMissing() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriterFactory factory = new RespReplyWriterFactory();

        ReplyWriter writer = factory.newWriter(null, sink);
        writer.booleanValue(true);

        Assert.assertEquals(":1\r\n", sink.utf8());
    }

    private static ServerSession serverSession(int respVersion) {
        return new MutableSession(respVersion);
    }

    private static final class MutableSession implements ServerSession {
        private int respVersion;

        private MutableSession(int respVersion) {
            this.respVersion = respVersion;
        }

            private final TransactionState transaction = new TransactionState() {
                @Override
                public boolean active() {
                    return false;
                }

                @Override
                public void begin() {
                }

                @Override
                public void discard() {
                }

                @Override
                public void enqueue(ExecutionRequest request) {
                }

                @Override
                public int size() {
                    return 0;
                }

                @Override
                public List<ExecutionRequest> drain() {
                    return List.of();
                }
            };

            private final ConnectionStatsView stats = new ConnectionStatsView() {
                @Override
                public int pending() {
                    return 0;
                }

                @Override
                public long pendingBytes() {
                    return 0;
                }

                @Override
                public boolean inputDisabledByExecutor() {
                    return false;
                }

                @Override
                public boolean closing() {
                    return false;
                }

                @Override
                public long commandsEnqueued() {
                    return 0;
                }

                @Override
                public long commandsExecuted() {
                    return 0;
                }

                @Override
                public long commandsRejected() {
                    return 0;
                }

                @Override
                public long commandsSkippedClosing() {
                    return 0;
                }

                @Override
                public long closeAfterReply() {
                    return 0;
                }

                @Override
                public long backpressureEnter() {
                    return 0;
                }

                @Override
                public long backpressureExit() {
                    return 0;
                }
            };

            @Override
            public int dbIndex() {
                return 0;
            }

            @Override
            public void setDbIndex(int dbIndex) {
            }

            @Override
            public long clientId() {
                return 1L;
            }

            @Override
            public String clientName() {
                return null;
            }

            @Override
            public void setClientName(String clientName) {
            }

            @Override
            public boolean authenticated() {
                return false;
            }

            @Override
            public void setAuthenticated(boolean authenticated) {
            }

            @Override
            public TransactionState transaction() {
                return transaction;
            }

            @Override
            public ConnectionStatsView connectionStats() {
                return stats;
            }

            @Override
            public int respVersion() {
                return respVersion;
            }

            @Override
            public void setRespVersion(int respVersion) {
                this.respVersion = respVersion;
            }
    }

    private static final class ByteArraySink implements BytesSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int off, int len) {
            out.write(src, off, len);
        }

        String utf8() {
            return out.toString(StandardCharsets.UTF_8);
        }
    }
}
