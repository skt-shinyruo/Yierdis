package yier.bubu.redis.app.bench;

/** benchmark 两个子命令共用的 argv 值转换（无 picocli）。 */
public final class BenchArgv {
    private BenchArgv() {
    }

    public static IllegalArgumentException unknown(String name, int index) {
        if (name.startsWith("--")) {
            return new IllegalArgumentException("Unknown option: '" + name + "'");
        }
        return new IllegalArgumentException("Unmatched argument at index " + index + ": '" + name + "'");
    }

    public static int intValue(String name, String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not an int");
        }
    }

    public static long longValue(String name, String raw) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not a long");
        }
    }

    public static boolean booleanValue(String name, String raw) {
        String normalized = raw.trim();
        if (normalized.equalsIgnoreCase("true")) {
            return true;
        }
        if (normalized.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not a boolean");
    }
}
