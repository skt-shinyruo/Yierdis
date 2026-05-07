package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionRequest;

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
