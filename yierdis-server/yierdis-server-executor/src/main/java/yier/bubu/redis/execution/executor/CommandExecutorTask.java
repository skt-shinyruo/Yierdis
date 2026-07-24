package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

final class CommandExecutorTask<C extends ExecutionConnection> {
    final C connection;
    final ExecutionRequest request;
    final int retainedBytes;
    final ExecutionReply reply;
    private CapacityRegistration capacityRegistration = CapacityRegistration.NONE;

    CommandExecutorTask(C connection, ExecutionRequest request, int retainedBytes, ExecutionReply reply) {
        this.connection = connection;
        this.request = request;
        this.retainedBytes = retainedBytes;
        this.reply = reply;
    }

    void ownCapacityRegistration(CapacityRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        CapacityRegistration previous = capacityRegistration;
        capacityRegistration = registration;
        previous.cancel();
    }

    void capacityRegistrationSignalled() {
        capacityRegistration = CapacityRegistration.NONE;
    }

    void cancelCapacityRegistration() {
        CapacityRegistration registration = capacityRegistration;
        capacityRegistration = CapacityRegistration.NONE;
        registration.cancel();
    }
}
