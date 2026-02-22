package yier.bubu.redis.protocol.reply;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * IR array 值。
 */
public final class ReplyArray implements ReplyValue {
    private final List<ReplyValue> values;

    public ReplyArray(List<ReplyValue> values) {
        Objects.requireNonNull(values, "values");
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public List<ReplyValue> values() {
        return values;
    }
}
