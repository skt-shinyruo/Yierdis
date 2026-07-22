package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.CommandSession;

public interface ExecutionConnection {
    String connectionId();

    CommandSession session();

    ExecutionConnectionContext context();

    boolean markClosing();
}
