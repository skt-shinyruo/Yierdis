package yier.bubu.redis.execution.api;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ReplyShapeTest {
    @Test
    public void aggregateRetainedBytesComeOnlyFromSemanticChildren() {
        ReplyShape shape = ReplyShapes.array(List.of(
                ReplyShapes.bulkString(3, 11),
                ReplyShapes.integer(7),
                ReplyShapes.sequence(2, 13, consumer -> {
                    consumer.accept(1);
                    consumer.accept(-1);
                })
        ));

        Assert.assertEquals(24L, shape.retainedSourceBytes());
    }

    @Test
    public void semanticLengthViewIsRepeatableAndKeepsNullSemantic() {
        ReplyShape.ByteSequence sequence = (ReplyShape.ByteSequence) ReplyShapes.sequence(
                3, 9, consumer -> {
                    consumer.accept(2);
                    consumer.accept(-1);
                    consumer.accept(5);
                });
        List<Integer> first = new ArrayList<>();
        List<Integer> second = new ArrayList<>();

        sequence.payloadLengths().visit(first::add);
        sequence.payloadLengths().visit(second::add);

        Assert.assertEquals(List.of(2, -1, 5), first);
        Assert.assertEquals(first, second);
    }

    @Test
    public void publicRecordConstructorsCannotBypassShapeValidation() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ReplyShape.SimpleString(-1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ReplyShape.BulkString(1, -1));
        Assert.assertThrows(NullPointerException.class,
                () -> new ReplyShape.ByteSequence(1, null, 0));

        List<ReplyShape> mutable = new ArrayList<>();
        mutable.add(new ReplyShape.IntegerValue(1));
        ReplyShape.Aggregate aggregate = new ReplyShape.Aggregate(
                ReplyShape.AggregateKind.ARRAY, mutable, 0);
        mutable.clear();

        Assert.assertEquals(1, aggregate.elements().size());
    }

    @Test
    public void errorFactoryCapturesTheNormalizedRedisErrorPayload() {
        ReplyShape.Error error = (ReplyShape.Error) ReplyShapes.error("wrong");

        Assert.assertEquals(9, error.payloadLength());
    }

}
