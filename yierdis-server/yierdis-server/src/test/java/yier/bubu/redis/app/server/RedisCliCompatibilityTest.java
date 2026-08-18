package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisCliCompatibilityTest {
    @Test
    public void redisCliCanPingSetGetAndNegotiateResp3() throws Exception {
        Assume.assumeTrue("redis-cli is not available", commandExists("redis-cli"));

        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0"
        )) {
            String port = Integer.toString(server.port());

            Assert.assertEquals("PONG", run("redis-cli", "-p", port, "PING").stdoutTrimmed());
            Assert.assertEquals("OK", run("redis-cli", "-p", port, "SET", "compat:key", "ok").stdoutTrimmed());
            Assert.assertEquals("ok", run("redis-cli", "-p", port, "GET", "compat:key").stdoutTrimmed());

            CommandResult hello = run("redis-cli", "--raw", "-3", "-p", port, "HELLO", "3");
            Assert.assertTrue(hello.stdout, containsHelloKey(hello.lines, "server"));
            Assert.assertTrue(hello.stdout, containsHelloField(hello.lines, "proto", "3"));
        }
    }

    private static boolean commandExists(String command) throws Exception {
        Process process = new ProcessBuilder("sh", "-lc", "command -v " + command)
                .redirectErrorStream(true)
                .start();
        boolean exited = process.waitFor(2, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            return false;
        }
        return process.exitValue() == 0;
    }

    private static CommandResult run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean exited = process.waitFor(5, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            Assert.fail("command timed out: " + Arrays.toString(command));
        }
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        String stdout = String.join("\n", lines);
        Assert.assertEquals("command failed: " + Arrays.toString(command) + "\n" + stdout, 0, process.exitValue());
        return new CommandResult(stdout, lines);
    }

    private static boolean containsHelloKey(List<String> lines, String key) {
        for (String line : lines) {
            if (key.equals(line) || line.startsWith(key + " ")) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsHelloField(List<String> lines, String key, String value) {
        for (int i = 0; i + 1 < lines.size(); i++) {
            if (key.equals(lines.get(i)) && value.equals(lines.get(i + 1))) {
                return true;
            }
        }
        for (String line : lines) {
            if ((key + " " + value).equals(line)) {
                return true;
            }
        }
        return false;
    }

    private record CommandResult(String stdout, List<String> lines) {
        private String stdoutTrimmed() {
            return stdout.trim();
        }
    }
}
