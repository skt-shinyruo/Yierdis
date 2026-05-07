package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.Session;

public interface ExecutionConnection {
    String connectionId();

    Session session();

    ExecutionConnectionContext context();

    boolean markClosing();
}
