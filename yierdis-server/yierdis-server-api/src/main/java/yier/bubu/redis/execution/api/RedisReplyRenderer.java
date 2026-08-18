package yier.bubu.redis.execution.api;

import java.util.Objects;

public final class RedisReplyRenderer {
    private RedisReplyRenderer() {
    }

    public static void render(RedisReply reply, RedisReplyWriter out) {
        Objects.requireNonNull(reply, "reply");
        Objects.requireNonNull(out, "out");
        switch (reply) {
            case RedisReply.SimpleString value -> out.simpleString(value.value());
            case RedisReply.Error value -> out.error(value.message());
            case RedisReply.ControlError value -> out.controlError(value.message());
            case RedisReply.IntegerValue value -> out.integer(value.value());
            case RedisReply.BulkString value -> value.emitter().accept(out);
            case RedisReply.NullValue ignored -> out.nullValue();
            case RedisReply.NullArray ignored -> out.nullArray();
            case RedisReply.Aggregate value -> renderAggregate(value, out);
            case RedisReply.ByteSequence value -> {
                out.arrayHeader(value.elementCount());
                value.emitter().accept(out);
            }
            case RedisReply.ByteSet value -> {
                out.setHeader(value.elementCount());
                value.emitter().accept(out);
            }
            case RedisReply.ByteMap value -> {
                out.mapHeader(value.pairCount());
                value.emitter().accept(out);
            }
        }
    }

    private static void renderAggregate(RedisReply.Aggregate aggregate, RedisReplyWriter out) {
        int elementCount = aggregate.elements().size();
        switch (aggregate.kind()) {
            case ARRAY -> out.arrayHeader(elementCount);
            case MAP -> out.mapHeader(elementCount / 2);
        }
        for (RedisReply element : aggregate.elements()) {
            render(element, out);
        }
    }
}
