package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BenchScriptContractTest {
    private static final List<String> BENCH_ENVIRONMENT = List.of(
            "SKIP_BUILD", "MVN_ARGS", "BENCH_JVM_OPTS", "HOST", "PORT",
            "REQUESTS", "CLIENTS", "DATA_SIZE", "PIPELINE", "FORMAT",
            "KEYSPACE", "TESTS", "KEEP_ALIVE", "PRECISION", "SEED",
            "BENCH_USERNAME", "USERNAME", "PASSWORD", "DATABASE"
    );

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void defaultInvocationBuildsBenchmarkAndIgnoresNewerClassifiedJars() throws Exception {
        ScriptFixture fixture = createScriptFixture(true, true);

        ScriptResult result = runScript(fixture, Map.of());

        Assert.assertEquals(result.output(), 0, result.exitCode());
        Assert.assertEquals(List.of(
                "-pl", "yierdis-benchmark", "-am", "-q", "-DskipTests", "package"
        ), readNulArguments(fixture.mvnLog()));
        Assert.assertEquals(List.of(
                "-jar", fixture.mainJar().toString(),
                "--host", "127.0.0.1",
                "--port", "16378",
                "--requests", "100000",
                "--clients", "50",
                "--data-size", "3",
                "--pipeline", "1",
                "--format", "human"
        ), readNulArguments(fixture.javaLog()));
    }

    @Test
    public void customInvocationSkipsBuildAndForwardsPortableArgumentsExactly() throws Exception {
        ScriptFixture fixture = createScriptFixture(true, false);
        String username = "acl user;$(not executed)";
        String password = "s e c r e t;$(not executed)";

        ScriptResult result = runScript(fixture, Map.ofEntries(
                Map.entry("SKIP_BUILD", "1"),
                Map.entry("BENCH_JVM_OPTS", "-Xms64m -Dbench.mode=edge"),
                Map.entry("HOST", "benchmark.example"),
                Map.entry("PORT", "6380"),
                Map.entry("REQUESTS", "321"),
                Map.entry("CLIENTS", "12"),
                Map.entry("DATA_SIZE", "4096"),
                Map.entry("PIPELINE", "8"),
                Map.entry("FORMAT", "csv"),
                Map.entry("KEYSPACE", "0"),
                Map.entry("TESTS", "ping,set,get"),
                Map.entry("KEEP_ALIVE", "false"),
                Map.entry("PRECISION", "4"),
                Map.entry("SEED", "42"),
                Map.entry("BENCH_USERNAME", username),
                Map.entry("USERNAME", "reserved-shell-user"),
                Map.entry("PASSWORD", password),
                Map.entry("DATABASE", "2")
        ));

        Assert.assertEquals(result.output(), 0, result.exitCode());
        Assert.assertFalse("mvn must not run when SKIP_BUILD=1", Files.exists(fixture.mvnLog()));
        Assert.assertEquals(List.of(
                "-Xms64m", "-Dbench.mode=edge",
                "-jar", fixture.mainJar().toString(),
                "--host", "benchmark.example",
                "--port", "6380",
                "--requests", "321",
                "--clients", "12",
                "--data-size", "4096",
                "--pipeline", "8",
                "--format", "csv",
                "--keyspace", "0",
                "--tests", "ping,set,get",
                "--keep-alive=false",
                "--precision", "4",
                "--seed", "42",
                "--username", username,
                "--password", password,
                "--database", "2"
        ), readNulArguments(fixture.javaLog()));
    }

    @Test
    public void missingMainBenchmarkJarFailsBeforeJavaStarts() throws Exception {
        ScriptFixture fixture = createScriptFixture(false, true);

        ScriptResult result = runScript(fixture, Map.of("SKIP_BUILD", "1"));

        Assert.assertNotEquals(0, result.exitCode());
        Assert.assertTrue(result.output(), result.output().contains("shaded yierdis-benchmark jar not found"));
        Assert.assertFalse("java must not run without the main jar", Files.exists(fixture.javaLog()));
        Assert.assertFalse("mvn must not run when SKIP_BUILD=1", Files.exists(fixture.mvnLog()));
    }

    @Test
    public void repoRootDiscoveryAcceptsExplicitRootAndWalksFallbackCandidates() throws IOException {
        Path explicitRoot = createRepositoryRoot("explicit-root");
        Path fallbackRoot = createRepositoryRoot("fallback-root");
        Path nestedFallback = Files.createDirectories(fallbackRoot.resolve("one/two"));
        Path invalid = temporaryFolder.newFolder("invalid-root").toPath();

        Assert.assertEquals(explicitRoot, discoverRepoRoot(explicitRoot, invalid));
        Assert.assertEquals(fallbackRoot, discoverRepoRoot(invalid, nestedFallback));
        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> discoverRepoRoot(invalid, invalid.resolve("missing"))
        );
        Assert.assertTrue(failure.getMessage().contains("repository root"));
    }

    private static Path repoRoot() {
        return discoverRepoRoot(
                propertyPath("maven.multiModuleProjectDirectory"),
                propertyPath("basedir"),
                propertyPath("user.dir")
        );
    }

    static Path discoverRepoRoot(Path multiModuleProjectDirectory, Path... fallbackCandidates) {
        Path configuredRoot = normalize(multiModuleProjectDirectory);
        if (isRepositoryRoot(configuredRoot)) {
            return configuredRoot;
        }
        for (Path candidate : fallbackCandidates) {
            for (Path current = normalize(candidate); current != null; current = current.getParent()) {
                if (isRepositoryRoot(current)) {
                    return current;
                }
            }
        }
        throw new IllegalStateException(
                "Unable to locate repository root containing pom.xml, scripts/bench.sh, "
                        + "and yierdis-benchmark/pom.xml"
        );
    }

    private ScriptFixture createScriptFixture(boolean includeMainJar, boolean includeClassifiers)
            throws IOException {
        Path root = temporaryFolder.newFolder().toPath().toAbsolutePath().normalize();
        Path scriptDirectory = Files.createDirectories(root.resolve("scripts"));
        Path script = scriptDirectory.resolve("bench.sh");
        Files.copy(repoRoot().resolve("scripts/bench.sh"), script);
        makeExecutable(script);

        Path bin = Files.createDirectories(root.resolve("bin"));
        writeExecutable(bin.resolve("mvn"), """
                #!/usr/bin/env bash
                set -euo pipefail
                printf '%s\\000' "$@" > "$MVN_LOG"
                """);
        writeExecutable(bin.resolve("java"), """
                #!/usr/bin/env bash
                set -euo pipefail
                printf '%s\\000' "$@" > "$JAVA_LOG"
                """);

        Path target = Files.createDirectories(root.resolve("yierdis-benchmark/target"));
        long now = System.currentTimeMillis();
        Path mainJar = target.resolve("yierdis-benchmark-0.1.0-SNAPSHOT.jar");
        if (includeMainJar) {
            createCandidate(mainJar, now - 5_000);
        }
        createCandidate(
                target.resolve("original-yierdis-benchmark-0.1.0-SNAPSHOT.jar"),
                now - 4_000
        );
        if (includeClassifiers) {
            createCandidate(target.resolve("yierdis-benchmark-0.1.0-SNAPSHOT-sources.jar"), now - 3_000);
            createCandidate(target.resolve("yierdis-benchmark-0.1.0-SNAPSHOT-javadoc.jar"), now - 2_000);
            createCandidate(target.resolve("yierdis-benchmark-0.1.0-SNAPSHOT-tests.jar"), now - 1_000);
            createCandidate(target.resolve("yierdis-benchmark-0.1.0-SNAPSHOT-profiled.jar"), now);
        }
        return new ScriptFixture(
                root,
                script,
                bin,
                root.resolve("mvn.args"),
                root.resolve("java.args"),
                mainJar
        );
    }

    private static ScriptResult runScript(ScriptFixture fixture, Map<String, String> overrides)
            throws Exception {
        ProcessBuilder builder = new ProcessBuilder(fixture.script().toString())
                .directory(fixture.root().toFile())
                .redirectErrorStream(true);
        Map<String, String> environment = builder.environment();
        BENCH_ENVIRONMENT.forEach(environment::remove);
        environment.put("PATH", fixture.bin() + System.getProperty("path.separator")
                + System.getenv("PATH"));
        environment.put("MVN_LOG", fixture.mvnLog().toString());
        environment.put("JAVA_LOG", fixture.javaLog().toString());
        environment.putAll(overrides);

        Process process = builder.start();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            Assert.fail("bench.sh did not exit within 10 seconds");
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new ScriptResult(process.exitValue(), output);
    }

    private static List<String> readNulArguments(Path log) throws IOException {
        Assert.assertTrue("argument log does not exist: " + log, Files.isRegularFile(log));
        byte[] bytes = Files.readAllBytes(log);
        List<String> arguments = new ArrayList<>();
        int start = 0;
        for (int index = 0; index < bytes.length; index++) {
            if (bytes[index] == 0) {
                arguments.add(new String(bytes, start, index - start, StandardCharsets.UTF_8));
                start = index + 1;
            }
        }
        Assert.assertEquals("argument log must end with NUL", bytes.length, start);
        return arguments;
    }

    private Path createRepositoryRoot(String name) throws IOException {
        Path root = temporaryFolder.newFolder(name).toPath().toAbsolutePath().normalize();
        Files.writeString(root.resolve("pom.xml"), "<project/>\n");
        Files.createDirectories(root.resolve("scripts"));
        Files.writeString(root.resolve("scripts/bench.sh"), "#!/usr/bin/env bash\n");
        Files.createDirectories(root.resolve("yierdis-benchmark"));
        Files.writeString(root.resolve("yierdis-benchmark/pom.xml"), "<project/>\n");
        return root;
    }

    private static void createCandidate(Path path, long modifiedMillis) throws IOException {
        Files.write(path, new byte[0]);
        Files.setLastModifiedTime(path, FileTime.fromMillis(modifiedMillis));
    }

    private static void writeExecutable(Path path, String content) throws IOException {
        Files.writeString(path, content, StandardCharsets.UTF_8);
        makeExecutable(path);
    }

    private static void makeExecutable(Path path) throws IOException {
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
    }

    private static Path propertyPath(String name) {
        String value = System.getProperty(name);
        return value == null || value.isBlank() ? null : Path.of(value);
    }

    private static Path normalize(Path path) {
        return path == null ? null : path.toAbsolutePath().normalize();
    }

    private static boolean isRepositoryRoot(Path path) {
        return path != null
                && Files.isRegularFile(path.resolve("pom.xml"))
                && Files.isRegularFile(path.resolve("scripts/bench.sh"))
                && Files.isRegularFile(path.resolve("yierdis-benchmark/pom.xml"));
    }

    private record ScriptFixture(
            Path root,
            Path script,
            Path bin,
            Path mvnLog,
            Path javaLog,
            Path mainJar
    ) {
    }

    private record ScriptResult(int exitCode, String output) {
    }
}
