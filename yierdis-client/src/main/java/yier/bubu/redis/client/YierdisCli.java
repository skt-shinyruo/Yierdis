package yier.bubu.redis.client;

import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class YierdisCli {
    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        if (config.showHelp) {
            printHelp();
            return;
        }

        try (YierdisClient client = YierdisClient.connect(config.host, config.port)) {
            if (!config.commandArgs.isEmpty()) {
                RespObject resp = client.execute(config.commandArgs, config.timeoutMillis);
                printResp(resp, config.hex);
                if (resp instanceof RespError) {
                    System.exit(1);
                }
                return;
            }

            runRepl(client, config);
        }
    }

    private static void runRepl(YierdisClient client, Config config) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.print("yierdis> ");
            System.out.flush();
            String line = br.readLine();
            if (line == null) {
                return;
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                // Best-effort QUIT to mirror redis-cli behavior, then exit.
                try {
                    client.execute(Arrays.asList(b("QUIT")), config.timeoutMillis);
                } catch (Exception ignored) {
                }
                return;
            }

            List<byte[]> cmd = parseArgsToUtf8Bytes(line);
            if (cmd.isEmpty()) {
                continue;
            }

            try {
                RespObject resp = client.execute(cmd, config.timeoutMillis);
                printResp(resp, config.hex);
            } catch (Exception e) {
                System.err.println("(error) " + e.getMessage());
            }
        }
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("  java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar [options] [COMMAND [ARG...]]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --host <host>            Default: 127.0.0.1");
        System.out.println("  --port <port>            Default: 6378");
        System.out.println("  --timeoutMillis <ms>     Default: 5000");
        System.out.println("  --hex                    Print bulk strings as hex bytes");
        System.out.println("  -h, --help               Show help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar PING");
        System.out.println("  java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar SET a 1");
        System.out.println("  java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar --port 6378 GET a");
    }

    private static void printResp(RespObject obj, boolean hex) {
        if (obj == null) {
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
        System.out.println(obj.toHumanReadableString());
    }

    private static void printInline(RespObject obj, boolean hex, int indent) {
        if (obj == null) {
            System.out.println("(nil)");
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
        List<String> parts = splitShellLike(line);
        List<byte[]> out = new ArrayList<>(parts.size());
        for (String p : parts) {
            out.add(p.getBytes(StandardCharsets.UTF_8));
        }
        return out;
    }

    private static List<String> splitShellLike(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        boolean escaping = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (escaping) {
                cur.append(c);
                escaping = false;
                continue;
            }
            if (c == '\\') {
                escaping = true;
                continue;
            }
            if (!inDouble && c == '\'' ) {
                inSingle = !inSingle;
                continue;
            }
            if (!inSingle && c == '"') {
                inDouble = !inDouble;
                continue;
            }
            if (!inSingle && !inDouble && Character.isWhitespace(c)) {
                if (cur.length() > 0) {
                    out.add(cur.toString());
                    cur.setLength(0);
                }
                continue;
            }
            cur.append(c);
        }
        if (escaping) {
            cur.append('\\');
        }
        if (cur.length() > 0) {
            out.add(cur.toString());
        }
        return out;
    }

    private static final class Config {
        final String host;
        final int port;
        final long timeoutMillis;
        final boolean hex;
        final boolean showHelp;
        final List<byte[]> commandArgs;

        private Config(String host, int port, long timeoutMillis, boolean hex, boolean showHelp, List<byte[]> commandArgs) {
            this.host = host;
            this.port = port;
            this.timeoutMillis = timeoutMillis;
            this.hex = hex;
            this.showHelp = showHelp;
            this.commandArgs = commandArgs;
        }

        static Config parse(String[] args) {
            String host = "127.0.0.1";
            int port = 6378;
            long timeoutMillis = 5000;
            boolean hex = false;
            boolean showHelp = false;

            int i = 0;
            while (i < args.length) {
                String a = args[i];
                if ("-h".equals(a) || "--help".equals(a)) {
                    showHelp = true;
                    i++;
                    continue;
                }
                if ("--hex".equals(a)) {
                    hex = true;
                    i++;
                    continue;
                }
                if ("--host".equals(a) && i + 1 < args.length) {
                    host = args[++i];
                    i++;
                    continue;
                }
                if ("--port".equals(a) && i + 1 < args.length) {
                    port = Integer.parseInt(args[++i]);
                    i++;
                    continue;
                }
                if ("--timeoutMillis".equals(a) && i + 1 < args.length) {
                    timeoutMillis = Long.parseLong(args[++i]);
                    i++;
                    continue;
                }
                break;
            }

            List<byte[]> cmd = new ArrayList<>();
            for (; i < args.length; i++) {
                cmd.add(args[i].getBytes(StandardCharsets.UTF_8));
            }

            return new Config(host, port, timeoutMillis, hex, showHelp, cmd);
        }
    }

    private YierdisCli() {
    }
}
