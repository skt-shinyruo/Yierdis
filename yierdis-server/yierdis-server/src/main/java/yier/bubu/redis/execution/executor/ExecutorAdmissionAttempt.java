package yier.bubu.redis.execution.executor;

import java.util.Objects;

public sealed interface ExecutorAdmissionAttempt<C extends ExecutionConnection>
        permits ExecutorAdmissionAttempt.Acquired,
                ExecutorAdmissionAttempt.Unavailable,
                ExecutorAdmissionAttempt.Rejected {
    record Acquired<C extends ExecutionConnection>(ExecutorAdmission<C> admission)
            implements ExecutorAdmissionAttempt<C> {
        public Acquired {
            Objects.requireNonNull(admission, "admission");
        }
    }

    record Unavailable<C extends ExecutionConnection>(BlockReason reason)
            implements ExecutorAdmissionAttempt<C> {
        public Unavailable {
            Objects.requireNonNull(reason, "reason");
        }
    }

    record Rejected<C extends ExecutionConnection>(CommandExecutor.SubmitRejectReason reason)
            implements ExecutorAdmissionAttempt<C> {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    enum BlockReason {
        QUEUE_SLOTS,
        QUEUE_BYTES
    }
}
