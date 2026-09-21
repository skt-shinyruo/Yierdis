package yier.bubu.redis.command.defaults;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandLimits;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

public final class CommandSupport {
    private final YierdisDbRouter dbRouter;
    private final ServerInfoProvider infoProvider;
    private final SlowCommandLimits slowCommandLimits;

    CommandSupport(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, SlowCommandLimits slowCommandLimits) {
        this.dbRouter = Objects.requireNonNull(dbRouter, "dbRouter");
        this.infoProvider = infoProvider;
        this.slowCommandLimits = Objects.requireNonNull(slowCommandLimits, "slowCommandLimits");
    }

    public DbEngine commandDb(CommandSession session) {
        Objects.requireNonNull(session, "session");
        return dbRouter.dbFor(session);
    }

    public int databases() {
        return dbRouter.databases();
    }

    public ServerInfoProvider infoProvider() {
        return infoProvider;
    }

    public SlowCommandLimits slowCommandLimits() {
        return slowCommandLimits;
    }

    public static PreparedCommand preparedMutation(
            ReplyShape reservationShape,
            PreparedMutation<?> mutation,
            Function<CommandSession, CommandResult> action
    ) {
        return PreparedCommands.ownedAction(
                reservationShape,
                mutation,
                () -> mutation.isCurrent() ? ValidationResult.VALID : ValidationResult.STALE,
                context -> translateExpectedCommandFailure(() -> action.apply(context))
        );
    }

    public static PreparedCommand preparedAction(
            ReplyShape reservationShape,
            Function<CommandSession, CommandResult> action
    ) {
        return PreparedCommands.action(
                reservationShape,
                context -> translateExpectedCommandFailure(() -> action.apply(context))
        );
    }

    static CommandResult translateExpectedCommandFailure(Supplier<CommandResult> action) {
        try {
            return action.get();
        } catch (WrongTypeException | YierdisCommandException failure) {
            return CommandResult.controlError(failure.getMessage());
        }
    }
}
