package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ReplySizer;
import yier.bubu.redis.execution.api.TransactionState;
import java.util.List;

public class RespReplySizerTest {
    @Test
    public void isTheProtocolOwnedReplySizer() {
        Assert.assertTrue(ReplySizer.class.isAssignableFrom(RespReplySizer.class));
    }

    @Test
    public void oneSemanticMapProducesExactResp2AndResp3Plans() {
        ReplyShape shape = ReplyShapes.byteMap(1, 17L, consumer -> {
            consumer.accept(1);
            consumer.accept(2);
        });
        TestCommandSession session = new TestCommandSession();
        RespReplySizer sizer = new RespReplySizer();

        session.setRespVersion(2);
        ReplyPlan resp2 = sizer.plan(session, shape);
        session.setRespVersion(3);
        ReplyPlan resp3 = sizer.plan(session, shape);

        Assert.assertEquals(19L, resp2.encodedUpperBoundBytes());
        Assert.assertEquals(19L, resp3.encodedUpperBoundBytes());
        Assert.assertEquals(17L, resp2.retainedSourceBytes());
        Assert.assertEquals(17L, resp3.retainedSourceBytes());
    }

    @Test
    public void semanticLengthCallbacksRejectInvalidValuesAndCardinality() {
        TestCommandSession session = new TestCommandSession();
        RespReplySizer sizer = new RespReplySizer();

        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.plan(
                session,
                new ReplyShape.ByteSequence(1, consumer -> consumer.accept(-2), 0)
        ));
        Assert.assertThrows(IllegalArgumentException.class, () -> sizer.plan(
                session,
                new ReplyShape.ByteMap(1, consumer -> consumer.accept(1), 0)
        ));
    }

    @Test
    public void scalarAndNullShapesUseTheActiveRespVersion() {
        assertEncodedBytes(2, ReplyShapes.simpleString("OK"), 5L);
        assertEncodedBytes(3, ReplyShapes.simpleString("OK"), 5L);
        assertEncodedBytes(2, ReplyShapes.error("wrong"), 12L);
        assertEncodedBytes(3, ReplyShapes.error("wrong"), 12L);
        assertEncodedBytes(2, ReplyShapes.integer(-42L), 6L);
        assertEncodedBytes(3, ReplyShapes.integer(-42L), 6L);
        assertEncodedBytes(2, ReplyShapes.booleanValue(true), 4L);
        assertEncodedBytes(3, ReplyShapes.booleanValue(true), 4L);
        assertEncodedBytes(2, ReplyShapes.doubleValue(1.5D), 9L);
        assertEncodedBytes(3, ReplyShapes.doubleValue(1.5D), 6L);
        assertEncodedBytes(2, ReplyShapes.bigNumber(" 123 "), 9L);
        assertEncodedBytes(3, ReplyShapes.bigNumber(" 123 "), 6L);
        assertEncodedBytes(2, ReplyShapes.verbatimString("a", 2), 8L);
        assertEncodedBytes(3, ReplyShapes.verbatimString("a", 2), 12L);
        assertEncodedBytes(2, ReplyShapes.blobError("bad"), 10L);
        assertEncodedBytes(3, ReplyShapes.blobError("bad"), 13L);
        assertEncodedBytes(2, ReplyShapes.nullValue(), 5L);
        assertEncodedBytes(3, ReplyShapes.nullValue(), 3L);
        assertEncodedBytes(2, ReplyShapes.nullArray(), 5L);
        assertEncodedBytes(3, ReplyShapes.nullArray(), 3L);
    }

    @Test
    public void aggregatesAndSemanticSequencesCountTheirNestedWireForms() {
        ReplyShape nested = ReplyShapes.array(List.of(
                ReplyShapes.bulkString(1, 0),
                ReplyShapes.map(List.of(ReplyShapes.bulkString(1, 0), ReplyShapes.integer(1)))
        ));
        ReplyShape sequence = ReplyShapes.sequence(2, 7L, consumer -> {
            consumer.accept(1);
            consumer.accept(-1);
        });

        assertEncodedBytes(2, nested, 26L);
        assertEncodedBytes(3, nested, 26L);
        assertEncodedBytes(2, sequence, 16L);
        assertEncodedBytes(3, sequence, 14L);
        Assert.assertEquals(7L, new RespReplySizer().plan(new TestCommandSession(), sequence)
                .retainedSourceBytes());
    }

    @Test
    public void alternativesReserveTheLargestWireRepresentation() {
        ReplyShape alternatives = ReplyShapes.oneOf(List.of(
                ReplyShapes.bulkString(3, 17L),
                ReplyShapes.error("wrong")
        ));

        ReplyPlan plan = new RespReplySizer().plan(new TestCommandSession(), alternatives);

        Assert.assertEquals(12L, plan.encodedUpperBoundBytes());
        Assert.assertEquals(17L, plan.retainedSourceBytes());
    }

    private static void assertEncodedBytes(int version, ReplyShape shape, long expected) {
        TestCommandSession session = new TestCommandSession();
        session.setRespVersion(version);

        Assert.assertEquals(expected, new RespReplySizer().plan(session, shape).encodedUpperBoundBytes());
    }

    private static final class TestCommandSession implements CommandSession {
        private int respVersion = 2;

        @Override
        public int dbIndex() {
            return 0;
        }

        @Override
        public void setDbIndex(int dbIndex) {
        }

        @Override
        public long clientId() {
            return 0L;
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
            return null;
        }

        @Override
        public ConnectionStatsView connectionStats() {
            return null;
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
}
