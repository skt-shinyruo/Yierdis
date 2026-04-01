package yier.bubu.redis.protocol.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Custom Protocol v1 decoded request model.
 * <p>
 * This is a protocol-layer DTO only; execution-layer {@code Command} adaptation happens in server-side wiring.
 */
public record CustomProtocolV1Request(String cmd, List<String> args) {
    public CustomProtocolV1Request {
        cmd = Objects.requireNonNull(cmd, "cmd");
        if (args == null || args.isEmpty()) {
            args = List.of();
        } else {
            args = Collections.unmodifiableList(new ArrayList<>(args));
        }
    }
}
