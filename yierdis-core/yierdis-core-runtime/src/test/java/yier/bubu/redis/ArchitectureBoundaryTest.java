package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ArchitectureBoundaryTest {
    @Test
    public void dbOpsAndCoreCommandMustNotImportProtocolModel() throws IOException {
        List<String> offenders = new ArrayList<>();
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        int scanned = 0;
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-api/src/main/java/yier/bubu/redis/ops"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到协议模型依赖泄漏（core-db/core-api/core-command 禁止 import yier.bubu.redis.protocol.*）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeMustNotOwnCommandAssemblyAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path instanceFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java"
        );
        Assert.assertTrue("缺少 YierdisInstance.java，无法执行 runtime 边界护栏", Files.isRegularFile(instanceFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                instanceFile,
                offenders,
                "import yier.bubu.redis.command.ServerInfoProvider;",
                "import yier.bubu.redis.command.SlowCommandGovernor;",
                "import yier.bubu.redis.command.YierdisFastCommandProcessor;",
                "new YierdisFastCommandProcessor(",
                "newCommandProcessor("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 core-runtime 重新承担命令处理器组装/装配职责（YierdisInstance 应只负责 DB 生命周期与路由）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreDefaultsMustNotOwnServerFacingCommands() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        Path serverCommands = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/ServerCommands.java"
        );
        if (Files.exists(serverCommands)) {
            offenders.add(relativePath(repoRoot, serverCommands) + " (server-facing commands should live in yierdis-server)");
        }

        Path processorFile = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java"
        );
        Assert.assertTrue("缺少 YierdisFastCommandProcessor.java，无法执行 core-command 默认装配护栏", Files.isRegularFile(processorFile));
        scanFileForForbiddenText(
                repoRoot,
                processorFile,
                offenders,
                "new ServerCommands(",
                "ERR HELLO is not allowed in MULTI",
                "asciiEqualsIgnoreCase(cmd, 0, \"HELLO\")"
        );
        scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                "register(\"HELLO\"",
                "register(\"INFO\"",
                "register(\"STATS\""
        );
        Path coreConnectionFile = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java"
        );
        Assert.assertTrue("缺少 CoreConnectionCommands.java，无法执行 COMMAND metadata 护栏", Files.isRegularFile(coreConnectionFile));
        scanFileForForbiddenText(
                repoRoot,
                coreConnectionFile,
                offenders,
                "case \"HELLO\":",
                "case \"INFO\":",
                "case \"STATS\":"
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server-facing commands 回流到 core 默认装配（这些命令应由 yierdis-server 组装）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreCommandMustNotReferenceLegacyWriteReservationApis() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                ".values()",
                ".eviction()",
                "prepareWrite(",
                "rollbackWriteReservationIfAny(",
                "DbMemoryConstants"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 core-command 仍依赖 legacy 写预留/混合 DB API：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void yierdisDbMustNotRetainLegacyReservationHelpers() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path dbFile = repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java");
        Assert.assertTrue("缺少 YierdisDb.java", Files.exists(dbFile));

        String source = Files.readString(dbFile);
        List<String> offenders = new ArrayList<>();
        if (source.contains("MemoryReservation activeReservation")) {
            offenders.add("activeReservation");
        }
        if (source.contains("public void ensureWriteAllowed(")) {
            offenders.add("ensureWriteAllowed(");
        }
        if (source.contains("public void prepareWrite(")) {
            offenders.add("prepareWrite(");
        }
        if (source.contains("public void rollbackWriteReservationIfAny(")) {
            offenders.add("rollbackWriteReservationIfAny(");
        }
        if (source.contains("void commitWrite(")) {
            offenders.add("commitWrite(");
        }
        if (source.contains("void rollbackWrite(")) {
            offenders.add("rollbackWrite(");
        }

        if (!offenders.isEmpty()) {
            Assert.fail("检测到 YierdisDb 仍保留 legacy reservation helper：\n" + String.join("\n", offenders));
        }
    }

    @Test
    public void runtimeMaintenanceMustNotCastPublicDbViewsBackToRuntime() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path runtimeAccessFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java，说明 runtime 显式访问 seam 未建立", Files.isRegularFile(runtimeAccessFile));

        Path maintenanceFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceMaintenance.java", Files.isRegularFile(maintenanceFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                maintenanceFile,
                offenders,
                "instanceof RuntimeDbEngine",
                "requireRuntimeEngine(",
                "DbEngine publicEngine = inst.engine("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 runtime maintenance 仍通过公开 DbEngine 视图回退到 RuntimeDbEngine：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverBootstrapMustNotInlineOwnerThreadLifecycleAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path runtimeAccessFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java，无法约束 bootstrap 生命周期边界", Files.isRegularFile(runtimeAccessFile));

        Path bootstrapFile = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java").normalize();
        Assert.assertTrue("缺少 YierdisServerBootstrap.java", Files.isRegularFile(bootstrapFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                bootstrapFile,
                offenders,
                "inst.bindToCurrentThread()",
                "inst.close()"
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 bootstrap 重新内联 owner-thread 生命周期逻辑，而不是通过 runtime seam 协作：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustOwnChannelAttrOnlyInServerConnectionContext() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path serverRoot = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis").normalize();
        Assert.assertTrue("缺少 yierdis-server/src/main/java/yier/bubu/redis", Files.isDirectory(serverRoot));

        Path allowedOwner = serverRoot.resolve("ServerConnectionContext.java");
        Assert.assertTrue("缺少 ServerConnectionContext.java，无法执行 Channel.attr 归一化护栏", Files.isRegularFile(allowedOwner));

        List<String> offenders = new ArrayList<>();
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(serverRoot)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .sorted()
                    .forEach(file -> {
                        if (allowedOwner.equals(file)) {
                            return;
                        }
                        scanned[0]++;
                        try {
                            scanFileForForbiddenText(
                                    repoRoot,
                                    file,
                                    offenders,
                                    ".attr(",
                                    "AttributeKey.valueOf("
                            );
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server Java 文件", scanned[0] > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server 存在分散的 Channel.attr 所有权（仅允许 ServerConnectionContext.java 持有）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    private static int scanForForbiddenText(Path repoRoot, Path root, List<String> offenders, String... forbiddenSnippets) throws IOException {
        if (root == null || !Files.exists(root)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .sorted()
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            scanFileForForbiddenText(repoRoot, p, offenders, forbiddenSnippets);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static void scanFileForForbiddenText(Path repoRoot, Path file, List<String> offenders, String... forbiddenSnippets)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            for (String snippet : forbiddenSnippets) {
                if (line.contains(snippet)) {
                    offenders.add(relativePath(repoRoot, file) + ":" + (i + 1) + " -> " + snippet);
                }
            }
        }
    }

    private static String relativePath(Path repoRoot, Path file) {
        if (repoRoot == null || file == null) {
            return String.valueOf(file);
        }
        return repoRoot.relativize(file).toString().replace('\\', '/');
    }

    private static Path resolveRepoRoot() {
        // Maven surefire 下通常为 yierdis-core 模块根目录；IDE/自定义运行环境下可能是仓库根目录。
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path direct = tryResolveRepoRoot(cwd);
        if (direct != null) {
            return direct;
        }

        String basedir = System.getProperty("basedir");
        if (basedir != null && !basedir.isBlank()) {
            Path byBasedir = tryResolveRepoRoot(Paths.get(basedir));
            if (byBasedir != null) {
                return byBasedir;
            }
        }

        Path p = cwd;
        for (int i = 0; i < 6 && p != null; i++) {
            Path candidate = tryResolveRepoRoot(p);
            if (candidate != null) {
                return candidate;
            }
            p = p.getParent();
        }
        return null;
    }

    private static Path tryResolveRepoRoot(Path base) {
        if (base == null) {
            return null;
        }

        if (Files.isDirectory(base.resolve("yierdis-core-api/src/main/java"))
                && Files.isDirectory(base.resolve("yierdis-core-db/src/main/java"))
                && Files.isRegularFile(base.resolve("pom.xml"))) {
            return base;
        }
        return null;
    }
}
