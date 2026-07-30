package yier.bubu.redis.testutil;

import java.util.Objects;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;

public final class TestPreparedCommands {
    private TestPreparedCommands() {
    }

    public static PreparedCommand simpleString(String value) {
        return PreparedCommands.ready(RedisReplies.simpleString(Objects.requireNonNull(value, "value")));
    }
}
