package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

public final class RespExecutionAdapter {
    public static final RespExecutionAdapter DEFAULT = new RespExecutionAdapter();

    public ExecutionRequest toExecutionRequest(RespCommandRequest request) {
        Objects.requireNonNull(request, "request");
        byte[][] argv = new byte[request.argc()][];
        for (int i = 0; i < argv.length; i++) {
            argv[i] = request.readOnlyArg(i);
        }
        return ByteArrayExecutionRequest.wrapReadOnly(argv, request.retainedBytes());
    }
}
