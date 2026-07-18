package yier.bubu.redis.app.bench.redis;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public record RedisBenchmarkCase(
        String id,
        String title,
        Set<String> selectionTriggers,
        RedisBenchmarkCommandTemplate template,
        Set<String> requiredCommands,
        BenchmarkReplyExpectation replyExpectation,
        Support support,
        String dependencyId
) {
    public RedisBenchmarkCase {
        id = normalizeRequired(id, "id", value -> value.toLowerCase(Locale.ROOT));
        title = normalizeRequired(title, "title", UnaryOperator.identity());
        selectionTriggers = normalizeRequiredSet(
                selectionTriggers,
                "selectionTriggers",
                value -> value.toLowerCase(Locale.ROOT)
        );
        template = Objects.requireNonNull(template, "template");
        requiredCommands = normalizeRequiredSet(
                requiredCommands,
                "requiredCommands",
                value -> value.toUpperCase(Locale.ROOT)
        );
        replyExpectation = Objects.requireNonNull(replyExpectation, "replyExpectation");
        support = Objects.requireNonNull(support, "support");
        dependencyId = dependencyId == null
                ? ""
                : dependencyId.trim().toLowerCase(Locale.ROOT);
    }

    public record Support(boolean supported, String reason) {
        public Support {
            reason = Objects.requireNonNull(reason, "reason").trim();
            if (supported && !reason.isEmpty()) {
                throw new IllegalArgumentException("supported cases must not declare a reason");
            }
            if (!supported && reason.isEmpty()) {
                throw new IllegalArgumentException("unsupported cases require a reason");
            }
        }

        public static Support available() {
            return new Support(true, "");
        }

        public static Support unsupported(String reason) {
            return new Support(false, reason);
        }
    }

    private static String normalizeRequired(String value, String name, UnaryOperator<String> normalizer) {
        String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalizer.apply(normalized);
    }

    private static Set<String> normalizeRequiredSet(
            Set<String> values,
            String name,
            UnaryOperator<String> normalizer
    ) {
        Objects.requireNonNull(values, name);
        if (values.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return values.stream()
                .map(value -> normalizeRequired(value, name + " element", normalizer))
                .collect(Collectors.toUnmodifiableSet());
    }
}
