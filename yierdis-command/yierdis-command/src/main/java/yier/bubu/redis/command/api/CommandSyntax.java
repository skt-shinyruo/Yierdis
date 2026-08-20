package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.ReplyAdmissionRequirement;

import java.util.Locale;
import java.util.Objects;

public record CommandSyntax(
        String nameUpper,
        CommandArity arity,
        CommandKeySpec keys,
        TransactionPolicy transactionPolicy,
        ReplyAdmissionRequirement replyAdmissionRequirement
) {
    public CommandSyntax {
        Objects.requireNonNull(nameUpper, "nameUpper");
        Objects.requireNonNull(arity, "arity");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(transactionPolicy, "transactionPolicy");
        Objects.requireNonNull(replyAdmissionRequirement, "replyAdmissionRequirement");
        nameUpper = nameUpper.trim().toUpperCase(Locale.ROOT);
        if (nameUpper.isEmpty() || !nameUpper.chars().allMatch(ch -> ch <= 0x7f)) {
            throw new IllegalArgumentException("command name must be non-empty ASCII");
        }
    }

    public CommandSyntax(
            String nameUpper,
            CommandArity arity,
            CommandKeySpec keys,
            TransactionPolicy transactionPolicy
    ) {
        this(nameUpper, arity, keys, transactionPolicy, ReplyAdmissionRequirement.PIPELINED);
    }

    public String nameLower() {
        return nameUpper.toLowerCase(Locale.ROOT);
    }
}
