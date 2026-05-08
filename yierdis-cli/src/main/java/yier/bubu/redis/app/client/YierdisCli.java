package yier.bubu.redis.app.client;

// CLI：提供简易的交互与单次执行，基于自定义协议 v1（request: <len>:<json>\n, reply: NDJSON）。

import picocli.CommandLine;
import yier.bubu.redis.protocol.custom.v1.wire.ProtocolLimits;
import yier.bubu.redis.protocol.custom.v1.json.JsonBoolean;
import yier.bubu.redis.protocol.custom.v1.json.JsonObject;
import yier.bubu.redis.protocol.custom.v1.json.JsonValue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class YierdisCli {
    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        YierdisCliArgs parsed = new YierdisCliArgs();
        CommandLine cmd = new CommandLine(parsed);
        cmd.setStopAtPositional(true);
        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            return 2;
        }

        if (parsed.help) {
            cmd.usage(System.out);
            return 0;
        }

        try (YierdisClient client = YierdisClient.connect(parsed.host, parsed.port)) {
            if (!parsed.command.isEmpty()) {
                List<byte[]> commandArgs = parsed.command.stream()
                        .map(s -> s == null ? null : s.getBytes(StandardCharsets.UTF_8))
                        .toList();

                YierdisClient.JsonReply reply = client.execute(commandArgs, parsed.timeoutMillis);
                printReply(reply, parsed.hex);
                return isOk(reply.envelope()) ? 0 : 1;
            }

            return runRepl(client, parsed);
        } catch (Exception e) {
            System.err.println("(error) " + e.getMessage());
            return 1;
        }
    }

    private static int runRepl(YierdisClient client, YierdisCliArgs config) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.print("yierdis> ");
            System.out.flush();
            String line = br.readLine();
            if (line == null) {
                return 0;
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                // Best-effort QUIT, then exit.
                try {
                    client.execute(Arrays.asList(b("QUIT")), config.timeoutMillis);
                } catch (Exception ignored) {
                }
                return 0;
            }

            final List<byte[]> cmd;
            try {
                cmd = parseArgsToUtf8Bytes(line);
            } catch (IllegalArgumentException e) {
                System.err.println("(error) " + e.getMessage());
                continue;
            }
            if (cmd.isEmpty()) {
                continue;
            }

            try {
                YierdisClient.JsonReply reply = client.execute(cmd, config.timeoutMillis);
                printReply(reply, config.hex);
            } catch (Exception e) {
                System.err.println("(error) " + e.getMessage());
            }
        }
    }

    private static void printReply(YierdisClient.JsonReply reply, boolean hex) {
        if (reply == null) {
            System.out.println("(nil)");
            return;
        }
        if (hex) {
            System.out.println(toHex(reply.line()));
            return;
        }
        System.out.println(reply.lineUtf8());
    }

    private static boolean isOk(JsonValue envelope) {
        if (!(envelope instanceof JsonObject obj)) {
            return false;
        }
        Map<String, JsonValue> map = obj.values();
        JsonValue ok = map.get("ok");
        return ok instanceof JsonBoolean b && b.value();
    }

    private static String toHex(byte[] data) {
        if (data == null) {
            return "(nil)";
        }
        StringBuilder sb = new StringBuilder(data.length * 2 + 2);
        sb.append("0x");
        for (byte b : data) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<byte[]> parseArgsToUtf8Bytes(String line) {
        // 保持与旧 CLI 一致的解析规则（sdssplitargs 风格）。
        return InlineCommandParser.splitUtf8(line, ProtocolLimits.DEFAULT_MAX_ARGS);
    }

    private YierdisCli() {
    }
}
