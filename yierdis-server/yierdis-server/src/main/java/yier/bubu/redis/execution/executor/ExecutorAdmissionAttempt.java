package yier.bubu.redis.execution.executor;

import java.util.Objects;

public sealed interface ExecutorAdmissionAttempt
        permits ExecutorAdmissionAttempt.Acquired,
                ExecutorAdmissionAttempt.Unavailable,
                ExecutorAdmissionAttempt.Rejected {
    record Acquired(ExecutorAdmission admission)
            implements ExecutorAdmissionAttempt {
        public Acquired {
            Objects.requireNonNull(admission, "admission");
        }
    }

    record Unavailable(BlockReason reason)
            implements ExecutorAdmissionAttempt {
        public Unavailable {
            Objects.requireNonNull(reason, "reason");
        }
    }

    record Rejected(CommandExecutor.SubmitRejectReason reason)
            implements ExecutorAdmissionAttempt {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    enum BlockReason {
        QUEUE_SLOTS,
        QUEUE_BYTES
    }
}
