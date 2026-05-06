package yier.bubu.redis.execution.executor;

import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Arrays;

final class TestExecutionRequests {
    private TestExecutionRequests() {
    }

    static ExecutionRequest ofUtf8(String... argv) {
        return ByteArrayExecutionRequest.fromUtf8(argv[0], Arrays.asList(Arrays.copyOfRange(argv, 1, argv.length)));
    }
}
