package yier.bubu.redis.testutil;

public final class ReplyInteger implements ReplyObject {
    private final long value;

    public ReplyInteger(long value) {
        this.value = value;
    }

    public long value() {
        return value;
    }
}

