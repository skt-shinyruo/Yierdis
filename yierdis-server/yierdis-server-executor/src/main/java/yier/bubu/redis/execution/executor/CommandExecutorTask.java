package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionRequest;

final class CommandExecutorTask<C extends ExecutionConnection> {
    final C connection;
    final ExecutionRequest request;
    final int retainedBytes;
    final ExecutionReply reply;

    CommandExecutorTask(C connection, ExecutionRequest request, int retainedBytes) {
        this(connection, request, retainedBytes, null);
    }

    CommandExecutorTask(C connection, ExecutionRequest request, int retainedBytes, ExecutionReply reply) {
        this.connection = connection;
        this.request = request;
        this.retainedBytes = retainedBytes;
        this.reply = reply;
    }
}
