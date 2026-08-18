package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

public interface ExecutionReply {
    ReplyReservationResult tryReserve(ReplyPlan plan);

    Runnable onCapacityAvailable(Runnable wakeup);

    BytesSink sink();

    void markReady(boolean closeAfterReply);

    void cancel();

    boolean hasWrittenBytes();

    void markResultUnknown();

}
