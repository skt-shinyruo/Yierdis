package yier.bubu.redis.client;

import picocli.CommandLine;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespLimits;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespInlineCommandParser;
import yier.bubu.redis.protocol.RespObjectParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

                try (RespFrame frame = client.execute(commandArgs, parsed.timeoutMillis)) {
                    RespObject resp = RespObjectParser.parse(frame);
                    printResp(resp, parsed.hex);
                    return resp instanceof RespError ? 1 : 0;
                }
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
                // Best-effort QUIT to mirror redis-cli behavior, then exit.
                try {
                    try (RespFrame ignored = client.execute(Arrays.asList(b("QUIT")), config.timeoutMillis)) {
                        // ignore
                    }
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
                try (RespFrame frame = client.execute(cmd, config.timeoutMillis)) {
                    RespObject resp = RespObjectParser.parse(frame);
                    printResp(resp, config.hex);
                }
            } catch (Exception e) {
                System.err.println("(error) " + e.getMessage());
            }
        }
    }

    private static void printResp(RespObject obj, boolean hex) {
        if (obj == null || obj instanceof RespNull) {
            System.out.println("(nil)");
            return;
        }
        if (obj instanceof RespSimpleString) {
            System.out.println(((RespSimpleString) obj).value());
            return;
        }
        if (obj instanceof RespError) {
            System.out.println("(error) " + ((RespError) obj).message());
            return;
        }
        if (obj instanceof RespInteger) {
            System.out.println("(integer) " + ((RespInteger) obj).value());
            return;
        }
        if (obj instanceof RespBulkString) {
            RespBulkString b = (RespBulkString) obj;
            if (b.isNull()) {
                System.out.println("(nil)");
                return;
            }
            if (hex) {
                System.out.println(toHex(b.data()));
                return;
            }
            System.out.println(b.asString());
            return;
        }
        if (obj instanceof RespArray) {
            RespArray arr = (RespArray) obj;
            if (arr.isNull()) {
                System.out.println("(nil)");
                return;
            }
            List<RespObject> values = arr.values();
            if (values == null || values.isEmpty()) {
                System.out.println("(empty array)");
                return;
            }
            for (int i = 0; i < values.size(); i++) {
                System.out.print((i + 1) + ") ");
                printInline(values.get(i), hex, 1);
            }
            return;
        }
        if (obj instanceof RespMap) {
            RespMap map = (RespMap) obj;
            if (map.entries().isEmpty()) {
                System.out.println("(empty map)");
                return;
            }
            int idx = 1;
            for (RespMap.Entry e : map.entries()) {
                System.out.print((idx++) + ") ");
                printInline(e.key(), hex, 1);
                System.out.print((idx++) + ") ");
                printInline(e.value(), hex, 1);
            }
            return;
        }
        System.out.println(obj.toHumanReadableString());
    }

    private static void printInline(RespObject obj, boolean hex, int indent) {
        if (obj == null || obj instanceof RespNull) {
            System.out.println("(nil)");
            return;
        }
        if (obj instanceof RespMap) {
            RespMap map = (RespMap) obj;
            if (map.entries().isEmpty()) {
                System.out.println("(empty map)");
                return;
            }
            System.out.println();
            for (int i = 0; i < map.entries().size(); i++) {
                for (int j = 0; j < indent; j++) {
                    System.out.print("  ");
                }
                System.out.print((i + 1) + ") ");
                printInline(map.entries().get(i).key(), hex, indent + 1);
                for (int j = 0; j < indent; j++) {
                    System.out.print("  ");
                }
                System.out.print("-> ");
                printInline(map.entries().get(i).value(), hex, indent + 1);
            }
            return;
        }
        if (obj instanceof RespArray) {
            RespArray arr = (RespArray) obj;
            if (arr.isNull() || arr.values() == null) {
                System.out.println("(nil)");
                return;
            }
            System.out.println();
            for (int i = 0; i < arr.values().size(); i++) {
                for (int j = 0; j < indent; j++) {
                    System.out.print("  ");
                }
                System.out.print((i + 1) + ") ");
                printInline(arr.values().get(i), hex, indent + 1);
            }
            return;
        }
        if (obj instanceof RespBulkString) {
            RespBulkString b = (RespBulkString) obj;
            if (b.isNull()) {
                System.out.println("(nil)");
                return;
            }
            System.out.println(hex ? toHex(b.data()) : b.asString());
            return;
        }
        if (obj instanceof RespSimpleString) {
            System.out.println(((RespSimpleString) obj).value());
            return;
        }
        if (obj instanceof RespError) {
            System.out.println("(error) " + ((RespError) obj).message());
            return;
        }
        if (obj instanceof RespInteger) {
            System.out.println("(integer) " + ((RespInteger) obj).value());
            return;
        }
        System.out.println(obj.toHumanReadableString());
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
        // 保持与 server inline command 相同的解析规则（sdssplitargs 风格）。
        return RespInlineCommandParser.splitUtf8(line, RespLimits.DEFAULT_MAX_ARGS);
    }

    private YierdisCli() {
    }
}
