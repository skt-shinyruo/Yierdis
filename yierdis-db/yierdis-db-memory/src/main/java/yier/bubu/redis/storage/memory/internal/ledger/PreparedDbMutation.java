package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.api.MutationOutcome;

public interface PreparedDbMutation<T> extends PreparedMutation<T> {
    MutationOutcome outcome();
}
