package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.ExecutionRequest;

/**
 * 把 argv 片段回显进 Redis 风格错误文案：NUL 截断该段、CR/LF→空格、非 ASCII→`?`。
 * unknown command、unknown subcommand、HELLO option 与 CLIENT SETINFO 未知属性共用，避免两套规则漂移。
 */
public final class RedisArgEcho {
    private RedisArgEcho() {
    }

    /** @return 写入的字符数（已计入 NUL 截断与替换） */
    public static int append(StringBuilder out, ExecutionRequest request, int argIndex, int maxChars) {
        int length = request.len(argIndex);
        int written = 0;
        for (int index = 0; index < length && written < maxChars; index++) {
            int value = request.byteAt(argIndex, index) & 0xff;
            if (value == 0) {
                break;
            }
            if (value == '\r' || value == '\n') {
                out.append(' ');
            } else if (value < 0x20 || value > 0x7e) {
                out.append('?');
            } else {
                out.append((char) value);
            }
            written++;
        }
        return written;
    }

    public static String echo(ExecutionRequest request, int argIndex, int maxChars) {
        StringBuilder out = new StringBuilder(Math.min(maxChars, 32));
        append(out, request, argIndex, maxChars);
        return out.toString();
    }
}
