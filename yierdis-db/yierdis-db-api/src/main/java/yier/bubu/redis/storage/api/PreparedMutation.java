package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.command.MutationContext;

public interface PreparedMutation<R> extends AutoCloseable {
    R preview();

    boolean isCurrent();

    MutationOutcome commit(MutationContext context);

    @Override
    void close();
}
