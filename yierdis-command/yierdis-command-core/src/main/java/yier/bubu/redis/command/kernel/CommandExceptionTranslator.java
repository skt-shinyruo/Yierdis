package yier.bubu.redis.command.kernel;

import java.util.Objects;
import java.util.function.Supplier;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

final class CommandExceptionTranslator {
    PreparedCommand prepare(Supplier<? extends PreparedCommand> action) {
        Objects.requireNonNull(action, "action");
        try {
            return Objects.requireNonNull(action.get(), "command preparer returned null");
        } catch (WrongTypeException e) {
            return PreparedCommands.error(e.getMessage());
        } catch (YierdisCommandException e) {
            return PreparedCommands.error(e.getMessage());
        } catch (IllegalArgumentException e) {
            return PreparedCommands.error("ERR " + e.getMessage());
        }
    }
}
