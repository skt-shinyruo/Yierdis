package yier.bubu.redis.app.client;

import java.util.ArrayList;
import java.util.List;

final class YierdisCliArgs {
    String host = "127.0.0.1";
    int port = 6378;
    long timeoutMillis = 5000;
    boolean hex;
    /** 第一个位置参数起全是命令单词（对应 picocli stopAtPositional 的语义）。 */
    final List<String> command = new ArrayList<>();

    /**
     * 手写 argv 解析（无 picocli）：选项只出现在第一个位置参数之前，
     * 其后所有单词都是待执行命令。支持 "--name value" 与 "--name=value"。
     */
    static YierdisCliArgs parse(String[] argv) {
        YierdisCliArgs args = new YierdisCliArgs();
        int i = 0;
        while (i < argv.length && argv[i].startsWith("--")) {
            String name = argv[i];
            String inlineValue = null;
            int eq = name.indexOf('=');
            if (eq >= 0) {
                inlineValue = name.substring(eq + 1);
                name = name.substring(0, eq);
            }
            switch (name) {
                case "--host":
                    args.host = value(argv, ++i, inlineValue, name);
                    break;
                case "--port":
                    args.port = intValue(name, value(argv, ++i, inlineValue, name));
                    break;
                case "--timeoutMillis":
                    args.timeoutMillis = longValue(name, value(argv, ++i, inlineValue, name));
                    break;
                case "--hex":
                    if (inlineValue != null) {
                        throw new IllegalArgumentException("option '--hex' does not take a value");
                    }
                    args.hex = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option: '" + name + "'");
            }
            i++;
        }
        for (; i < argv.length; i++) {
            args.command.add(argv[i]);
        }
        return args;
    }

    private static String value(String[] argv, int index, String inlineValue, String name) {
        if (inlineValue != null) {
            return inlineValue;
        }
        if (index >= argv.length) {
            throw new IllegalArgumentException("Missing required parameter for option '" + name + "'");
        }
        return argv[index];
    }

    private static int intValue(String name, String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not an int");
        }
    }

    private static long longValue(String name, String raw) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not a long");
        }
    }

    private YierdisCliArgs() {
    }
}
