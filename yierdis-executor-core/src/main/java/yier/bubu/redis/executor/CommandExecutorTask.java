package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ExecutionRequest;

final class CommandExecutorTask<C extends ExecutionConnection> {
    final C connection;
    final ExecutionRequest request;
    final int retainedBytes;

    CommandExecutorTask(C connection, ExecutionRequest request, int retainedBytes) {
        this.connection = connection;
        this.request = request;
        this.retainedBytes = retainedBytes;
    }
}
