package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyPlan;

import java.util.Objects;

final class CommandExecutorTask<C extends ExecutionConnection> {
    final C connection;
    final ExecutionRequest request;
    final int retainedBytes;
    final ExecutionReply reply;
    PreparedCommand prepared;
    ReplyPlan replyPlan;
    private Runnable capacityRegistration;

    CommandExecutorTask(C connection, ExecutionRequest request, int retainedBytes, ExecutionReply reply) {
        this.connection = connection;
        this.request = request;
        this.retainedBytes = retainedBytes;
        this.reply = reply;
    }

    void ownCapacityRegistration(Runnable registration) {
        Objects.requireNonNull(registration, "registration");
        Runnable previous = capacityRegistration;
        capacityRegistration = registration;
        if (previous != null) {
            previous.run();
        }
    }

    void cancelCapacityRegistration() {
        Runnable registration = capacityRegistration;
        capacityRegistration = null;
        if (registration != null) {
            registration.run();
        }
    }

    void closePrepared() {
        PreparedCommand owned = prepared;
        prepared = null;
        replyPlan = null;
        if (owned != null) {
            owned.close();
        }
    }
}
