package yier.bubu.redis.execution.executor;

import yier.bubu.redis.contract.Session;

public interface ExecutionConnection {
    String connectionId();

    Session session();

    ExecutionConnectionContext context();

    boolean markClosing();
}
