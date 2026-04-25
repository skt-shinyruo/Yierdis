package yier.bubu.redis.executor;

public interface ExecutionConnection {
    String connectionId();

    ExecutionConnectionContext context();
}
