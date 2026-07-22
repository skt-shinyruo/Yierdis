package yier.bubu.redis.command.api;

import java.util.Locale;
import java.util.Objects;

public record CommandSyntax(
        String nameUpper,
        CommandArity arity,
        CommandKeySpec keys,
        TransactionPolicy transactionPolicy
) {
    public CommandSyntax {
        Objects.requireNonNull(nameUpper, "nameUpper");
        Objects.requireNonNull(arity, "arity");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(transactionPolicy, "transactionPolicy");
        nameUpper = nameUpper.trim().toUpperCase(Locale.ROOT);
        if (nameUpper.isEmpty() || !nameUpper.chars().allMatch(ch -> ch <= 0x7f)) {
            throw new IllegalArgumentException("command name must be non-empty ASCII");
        }
    }

    public String nameLower() {
        return nameUpper.toLowerCase(Locale.ROOT);
    }
}
