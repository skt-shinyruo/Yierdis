package yier.bubu.redis.app.client;

// CLI：提供简易的交互与单次执行，使用 Redis RESP 协议。

import picocli.CommandLine;
import yier.bubu.redis.protocol.resp.InlineCommandParser;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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

                YierdisClient.RespReply reply = client.execute(commandArgs, parsed.timeoutMillis);
                printReply(reply, parsed.hex);
                return isSuccess(reply) ? 0 : 1;
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
                YierdisClient.RespReply reply = client.execute(cmd, config.timeoutMillis);
                printReply(reply, config.hex);
            } catch (Exception e) {
                System.err.println("(error) " + e.getMessage());
            }
        }
    }

    private static void printReply(YierdisClient.RespReply reply, boolean hex) {
        printReply(reply, hex, "");
    }

    private static void printReply(YierdisClient.RespReply reply, boolean hex, String prefix) {
        if (reply == null || reply.isNull()) {
            System.out.println(prefix + "(nil)");
            return;
        }

        switch (reply.kind()) {
            case SIMPLE_STRING -> System.out.println(prefix + reply.text());
            case ERROR -> System.out.println(prefix + "(error) " + reply.text());
            case INTEGER -> System.out.println(prefix + reply.integer());
            case BULK_STRING -> System.out.println(prefix + formatBulk(reply.bytes(), hex));
            case ARRAY -> printArray(reply, hex, prefix);
            case NULL -> System.out.println(prefix + "(nil)");
        }
    }

    private static void printArray(YierdisClient.RespReply reply, boolean hex, String prefix) {
        List<YierdisClient.RespReply> values = reply.values();
        if (values == null) {
            System.out.println(prefix + "(nil)");
            return;
        }
        for (int i = 0; i < values.size(); i++) {
            printReply(values.get(i), hex, prefix + (i + 1) + ") ");
        }
    }

    private static boolean isSuccess(YierdisClient.RespReply reply) {
        return reply != null && reply.kind() != YierdisClient.RespReply.Kind.ERROR;
    }

    private static String formatBulk(byte[] bytes, boolean hex) {
        if (bytes == null) {
            return "(nil)";
        }
        String utf8 = decodeUtf8(bytes);
        if (utf8 != null) {
            return utf8;
        }
        return hex ? toHex(bytes) : new String(bytes, StandardCharsets.UTF_8);
    }

    private static String decodeUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
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
        return InlineCommandParser.splitUtf8(line, RespProtocolLimits.DEFAULT_MAX_ARGS);
    }

    private YierdisCli() {
    }
}
