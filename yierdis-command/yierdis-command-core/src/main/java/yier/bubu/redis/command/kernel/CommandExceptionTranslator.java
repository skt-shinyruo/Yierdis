package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.util.Objects;

final class CommandExceptionTranslator {
    void run(RedisReplyWriter out, Runnable action) {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(action, "action");
        try {
            action.run();
        } catch (WrongTypeException e) {
            out.error(e.getMessage());
        } catch (YierdisCommandException e) {
            out.error(e.getMessage());
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        }
    }
}
