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
            // --name=value 的值已经在当前单词里。先 ++i 会把后面的命令单词跳过，
            // --port=16379 PING 就会变成空命令并进入 REPL。
            boolean inline = inlineValue != null;
            switch (name) {
                case "--host":
                    args.host = value(argv, i, inlineValue, name);
                    break;
                case "--port":
                    args.port = portValue(value(argv, i, inlineValue, name));
                    break;
                case "--timeoutMillis":
                    args.timeoutMillis = timeoutValue(value(argv, i, inlineValue, name));
                    break;
                case "--hex":
                    if (inline) {
                        throw new IllegalArgumentException("option '--hex' does not take a value");
                    }
                    args.hex = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option: '" + name + "'");
            }
            i += inline || "--hex".equals(name) ? 1 : 2;
        }
        for (; i < argv.length; i++) {
            args.command.add(argv[i]);
        }
        return args;
    }

    private static String value(String[] argv, int optionIndex, String inlineValue, String name) {
        if (inlineValue != null) {
            return inlineValue;
        }
        int valueIndex = optionIndex + 1;
        if (valueIndex >= argv.length) {
            throw new IllegalArgumentException("Missing required parameter for option '" + name + "'");
        }
        return argv[valueIndex];
    }

    private static int portValue(String raw) {
        int port = intValue("--port", raw);
        // 连 socket 之前就拒绝。InetSocketAddress 对同样的范围也会抛异常，但那已经走进连接路径，退出码会变成 1。
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid value for option '--port': '" + raw + "' is out of range");
        }
        return port;
    }

    private static long timeoutValue(String raw) {
        long timeout = longValue("--timeoutMillis", raw);
        // execute() 也会拒绝非正超时，但那发生在 connect() 之后。参数错误不能先把连接建起来。
        if (timeout <= 0) {
            throw new IllegalArgumentException(
                    "Invalid value for option '--timeoutMillis': '" + raw + "' is out of range");
        }
        return timeout;
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
