package yier.bubu.redis.command.api;

import java.util.Objects;

public record CommandSpec(CommandSyntax syntax, CommandHandler handler) {
    public CommandSpec {
        Objects.requireNonNull(syntax, "syntax");
        Objects.requireNonNull(handler, "handler");
    }
}
