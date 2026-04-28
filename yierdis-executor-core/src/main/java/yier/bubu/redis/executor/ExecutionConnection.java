package yier.bubu.redis.executor;

import yier.bubu.redis.contract.Session;

public interface ExecutionConnection {
    String connectionId();

    Session session();

    ExecutionConnectionContext context();

    boolean markClosing();
}
