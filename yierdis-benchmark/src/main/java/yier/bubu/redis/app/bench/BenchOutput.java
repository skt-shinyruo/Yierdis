package yier.bubu.redis.app.bench;

import java.util.Locale;

public final class BenchOutput {
    private BenchOutput() {
    }

    public static String format(String format, Object... arguments) {
        return String.format(Locale.ROOT, format, arguments);
    }

    public static void append(StringBuilder output, String format, Object... arguments) {
        output.append(format(format, arguments));
    }
}
