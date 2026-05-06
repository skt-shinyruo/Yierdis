package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

/**
 * Pure Custom Protocol v1 to execution-contract adapter.
 */
public final class CustomProtocolV1ExecutionAdapter {
    public static final CustomProtocolV1ExecutionAdapter DEFAULT = new CustomProtocolV1ExecutionAdapter();

    public ExecutionRequest toExecutionRequest(CustomProtocolV1ArgvRequest request) {
        Objects.requireNonNull(request, "request");
        byte[][] argv = new byte[request.argc()][];
        for (int i = 0; i < argv.length; i++) {
            argv[i] = request.readOnlyArg(i);
        }
        return ByteArrayExecutionRequest.wrapReadOnly(argv, request.retainedBytes());
    }

    public ExecutionRequest toExecutionRequest(CustomProtocolV1Request request) {
        Objects.requireNonNull(request, "request");
        return ByteArrayExecutionRequest.fromUtf8(request.cmd(), request.args());
    }
}
