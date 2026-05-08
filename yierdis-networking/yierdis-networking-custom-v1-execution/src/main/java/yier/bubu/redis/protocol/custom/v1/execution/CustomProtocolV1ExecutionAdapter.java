package yier.bubu.redis.protocol.custom.v1.execution;

import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.custom.v1.wire.CustomProtocolV1ArgvRequest;
import yier.bubu.redis.protocol.custom.v1.wire.CustomProtocolV1Request;

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
