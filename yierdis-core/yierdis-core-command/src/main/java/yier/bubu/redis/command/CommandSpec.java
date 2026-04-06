package yier.bubu.redis.command;

import java.util.Objects;

/**
 * Unified command registration shape: handler + metadata + MULTI policy.
 */
public record CommandSpec(
        CommandModule.Handler handler,
        CommandDescriptor descriptor,
        String disallowedInMultiError
) {
    public CommandSpec {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(descriptor, "descriptor");
    }
}
