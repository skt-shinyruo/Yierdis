package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

final class PreparedCommandTestSupport {
    private PreparedCommandTestSupport() {
    }

    static CommandSession newSession() {
        return new TestSession();
    }

    static void execute(
            YierdisFastCommandProcessor processor,
            ExecutionRequest request,
            RedisReplyWriter reply
    ) {
        execute(processor, newSession(), request, reply);
    }

    static void execute(
            YierdisFastCommandProcessor processor,
            CommandSession session,
            ExecutionRequest request,
            RedisReplyWriter reply
    ) {
        Objects.requireNonNull(processor, "processor");
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(reply, "reply");
        try (request;
             PreparedCommand prepared = processor.prepare(request, new CommandPreparationContext(session))) {
            Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
            try (CommandExecutionContext context = CommandExecutionContext.forRequest(session, reply, request)) {
                prepared.execute(context);
            }
        }
    }

    private static final class TestSession implements CommandSession {
        private final TransactionState tx = new TestTransactionState();

        @Override
        public int dbIndex() {
            return 0;
        }

        @Override
        public void setDbIndex(int dbIndex) {
        }

        @Override
        public long clientId() {
            return 1L;
        }

        @Override
        public String clientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {
        }

        @Override
        public boolean authenticated() {
            return false;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
        }

        @Override
        public TransactionState transaction() {
            return tx;
        }

        @Override
        public ConnectionStatsView connectionStats() {
            return null;
        }

        @Override
        public int respVersion() {
            return 2;
        }

        @Override
        public void setRespVersion(int respVersion) {
        }
    }

    private static final class TestTransactionState implements TransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public boolean aborted() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void markAborted() {
        }

        @Override
        public void discard() {
        }

        @Override
        public String tryEnqueue(ExecutionRequest request) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public List<ExecutionRequest> drain() {
            return List.of();
        }

        @Override
        public void forEachQueued(Consumer<? super ExecutionRequest> visitor) {
        }

        @Override
        public void close() {
        }
    }
}
