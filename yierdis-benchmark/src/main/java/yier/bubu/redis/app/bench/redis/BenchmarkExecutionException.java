package yier.bubu.redis.app.bench.redis;

import java.util.Objects;

public final class BenchmarkExecutionException extends Exception {
    private final long completedReplies;
    private final String detail;

    public BenchmarkExecutionException(
            String title,
            long completedReplies,
            int requestedReplies,
            String detail,
            Throwable cause
    ) {
        super(
                message(title, completedReplies, requestedReplies, detail),
                Objects.requireNonNull(cause, "cause")
        );
        this.completedReplies = completedReplies;
        this.detail = detail;
    }

    public long completedReplies() {
        return completedReplies;
    }

    public String detail() {
        return detail;
    }

    private static String message(
            String title,
            long completedReplies,
            int requestedReplies,
            String detail
    ) {
        String requiredTitle = Objects.requireNonNull(title, "title").trim();
        String requiredDetail = Objects.requireNonNull(detail, "detail");
        if (requiredTitle.isEmpty()) {
            throw new IllegalArgumentException("title must not be blank");
        }
        if (completedReplies < 0) {
            throw new IllegalArgumentException("completedReplies must be >= 0");
        }
        if (requestedReplies <= 0) {
            throw new IllegalArgumentException("requestedReplies must be > 0");
        }
        return requiredTitle + " failed after " + completedReplies + "/" + requestedReplies
                + " replies: " + requiredDetail;
    }
}
