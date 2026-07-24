package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

public interface ExecutionReply extends AutoCloseable {
    ReplyReservationResult tryReserve(ReplyPlan plan);

    CapacityRegistration onCapacityAvailable(Runnable wakeup);

    BytesSink sink();

    void markReady(boolean closeAfterReply);

    void cancel();

    boolean hasWrittenBytes();

    void markResultUnknown();

    @Override
    void close();
}
