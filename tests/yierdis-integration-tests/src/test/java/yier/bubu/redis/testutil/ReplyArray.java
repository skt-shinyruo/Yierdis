package yier.bubu.redis.testutil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ReplyArray implements ReplyObject {
    private final List<ReplyObject> values;

    public ReplyArray(List<ReplyObject> values) {
        Objects.requireNonNull(values, "values");
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public List<ReplyObject> values() {
        return values;
    }
}

