package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.List;

final class TestCommandContexts {
    private TestCommandContexts() {
    }

    static CommandContext context(RedisReplyWriter out) {
        return new CommandContext(new TestSession(), out);
    }

    private static final class TestSession implements ServerSession {
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
    }

    private static final class TestTransactionState implements TransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void discard() {
        }

        @Override
        public void enqueue(ExecutionRequest request) {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public List<ExecutionRequest> drain() {
            return List.of();
        }
    }
}
