package yier.bubu.redis.corecommand;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CoreCommandBoundaryGuardTest {
    @Test
    public void coreCommandMustBeNettyFreeAndMustNotImportCoreDbImplPackages() throws IOException {
        Path moduleRoot = resolveModuleRoot();
        Assert.assertNotNull("无法定位 yierdis-core-command 模块根目录（未找到 src/main/java）", moduleRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenImports(moduleRoot.resolve("src/main/java"), offenders);
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail("检测到 core-command 违规 import（禁止 import io.netty.* / yier.bubu.redis.db.*(impl)）：\n" + String.join("\n", offenders));
        }
    }

    @Test
    public void coreCommandMustNotContainLegacyWriteReservationFallbacks() throws IOException {
        Path moduleRoot = resolveModuleRoot();
        Assert.assertNotNull("无法定位 yierdis-core-command 模块根目录（未找到 src/main/java）", moduleRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                moduleRoot.resolve("src/main/java"),
                offenders,
                ".values()",
                ".eviction()",
                "prepareWrite(",
                "rollbackWriteReservationIfAny(",
                "DbMemoryConstants"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail("检测到 core-command 仍包含 legacy 写预留/混合 API 引用：\n" + String.join("\n", offenders));
        }
    }

    @Test
    public void commandDescriptorMustNotRetainFallbackMetadataTables() throws IOException {
        Path moduleRoot = resolveModuleRoot();
        Assert.assertNotNull("无法定位 yierdis-core-command 模块根目录（未找到 src/main/java）", moduleRoot);

        Path descriptorFile = moduleRoot.resolve("src/main/java/yier/bubu/redis/command/CommandDescriptor.java");
        Assert.assertTrue("缺少 CommandDescriptor.java", Files.isRegularFile(descriptorFile));

        String source = Files.readString(descriptorFile, StandardCharsets.UTF_8);
        Assert.assertFalse("CommandDescriptor 不应继续承担 fallback metadata table", source.contains("defaultForNameUpper("));
        Assert.assertFalse("CommandDescriptor 不应继续内联 switch metadata table", source.contains("switch (nameUpper)"));
    }

    @Test
    public void commandRegistryMustNotRetainFallbackMetadataTables() throws IOException {
        Path moduleRoot = resolveModuleRoot();
        Assert.assertNotNull("无法定位 yierdis-core-command 模块根目录（未找到 src/main/java）", moduleRoot);

        Path registryFile = moduleRoot.resolve("src/main/java/yier/bubu/redis/command/CommandRegistry.java");
        Assert.assertTrue("缺少 CommandRegistry.java", Files.isRegularFile(registryFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                registryFile,
                offenders,
                "defaultDescriptorForNameUpper(",
                "defaultArity(",
                "defaultFirstKeyIndex(",
                "defaultLastKeyIndex(",
                "defaultKeyStep("
        );
        if (!offenders.isEmpty()) {
            Assert.fail("CommandRegistry 不应继续保留 fallback metadata table：\n" + String.join("\n", offenders));
        }
    }

    @Test
    public void executeCommandCompatibilityOverloadMustBeExplicitlyDeprecated() throws NoSuchMethodException {
        Assert.assertTrue(
                "execute(Command, CommandContext) 必须显式标记为兼容层 @Deprecated overload",
                YierdisFastCommandProcessor.class
                        .getDeclaredMethod("execute", Command.class, CommandContext.class)
                        .isAnnotationPresent(Deprecated.class)
        );
    }

    @Test
    public void coreCommandMustOnlyUseCommandCompatibilitySurfacesTemporarily() throws IOException {
        Path moduleRoot = resolveModuleRoot();
        Assert.assertNotNull("无法定位 yierdis-core-command 模块根目录（未找到 src/main/java）", moduleRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenPattern(
                moduleRoot.resolve("src/main/java/yier/bubu/redis/command"),
                offenders,
                Pattern.compile("\\byier\\.bubu\\.redis\\.contract\\.Command\\b")
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        allowOnly(
                offenders,
                "src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java",
                "src/main/java/yier/bubu/redis/command/CommandSupport.java"
        );
        if (!offenders.isEmpty()) {
            Assert.fail("core-command 只能在 Task 2 允许的兼容面保留 Command 引用：\n" + String.join("\n", offenders));
        }

        Path supportFile = moduleRoot.resolve("src/main/java/yier/bubu/redis/command/CommandSupport.java");
        Assert.assertTrue("缺少 CommandSupport.java", Files.isRegularFile(supportFile));
        String source = Files.readString(supportFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "CommandSupport 必须显式说明当前 zero-copy Command fallback 是临时兼容 seam",
                source.contains("temporary compatibility seam for zero-copy/frame-backed Command producers")
        );
    }

    private static int scanForForbiddenImports(Path srcRoot, List<String> offenders) throws IOException {
        if (srcRoot == null || !Files.isDirectory(srcRoot)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(srcRoot)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
                            for (String line : lines) {
                                if (line == null) {
                                    continue;
                                }
                                String trimmed = line.trim();
                                if (trimmed.startsWith("import io.netty.")) {
                                    offenders.add(p.toString());
                                    break;
                                }
                                if (trimmed.startsWith("import yier.bubu.redis.db.")) {
                                    offenders.add(p.toString());
                                    break;
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanForForbiddenText(Path srcRoot, List<String> offenders, String... forbiddenSnippets) throws IOException {
        if (srcRoot == null || !Files.isDirectory(srcRoot)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(srcRoot)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
                            for (int i = 0; i < lines.size(); i++) {
                                String line = lines.get(i);
                                for (String snippet : forbiddenSnippets) {
                                    if (line.contains(snippet)) {
                                        offenders.add(p.toString() + ":" + (i + 1) + " -> " + snippet);
                                    }
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanForForbiddenPattern(Path srcRoot, List<String> offenders, Pattern forbiddenPattern) throws IOException {
        if (srcRoot == null || !Files.isDirectory(srcRoot)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(srcRoot)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
                            for (int i = 0; i < lines.size(); i++) {
                                String line = lines.get(i);
                                if (forbiddenPattern.matcher(line).find()) {
                                    offenders.add(p.toString() + ":" + (i + 1) + " -> " + forbiddenPattern.pattern());
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static void scanFileForForbiddenText(Path file, List<String> offenders, String... forbiddenSnippets) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            for (String snippet : forbiddenSnippets) {
                if (line.contains(snippet)) {
                    offenders.add(file.toString() + ":" + (i + 1) + " -> " + snippet);
                }
            }
        }
    }

    private static void allowOnly(List<String> offenders, String... allowedSuffixes) {
        if (offenders.isEmpty()) {
            return;
        }
        offenders.removeIf(offender -> {
            String path = offender;
            int marker = offender.indexOf(" -> ");
            if (marker >= 0) {
                path = offender.substring(0, marker);
            }
            int lineSep = path.lastIndexOf(':');
            if (lineSep > 1) {
                path = path.substring(0, lineSep);
            }
            for (String suffix : allowedSuffixes) {
                if (path.endsWith(suffix)) {
                    return true;
                }
            }
            return false;
        });
    }

    private static Path resolveModuleRoot() {
        // Maven surefire 下通常为 yierdis-core-command 模块根目录；IDE/自定义运行环境下可能是仓库根目录。
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path direct = tryResolveModuleRoot(cwd);
        if (direct != null) {
            return direct;
        }

        String basedir = System.getProperty("basedir");
        if (basedir != null && !basedir.isBlank()) {
            Path byBasedir = tryResolveModuleRoot(Paths.get(basedir));
            if (byBasedir != null) {
                return byBasedir;
            }
        }

        Path p = cwd;
        for (int i = 0; i < 6 && p != null; i++) {
            Path candidate = tryResolveModuleRoot(p);
            if (candidate != null) {
                return candidate;
            }
            p = p.getParent();
        }
        return null;
    }

    private static Path tryResolveModuleRoot(Path base) {
        if (base == null) {
            return null;
        }
        if (Files.isDirectory(base.resolve("src/main/java"))) {
            return base;
        }
        Path nested = base.resolve("yierdis-core-command");
        if (Files.isDirectory(nested.resolve("src/main/java"))) {
            return nested;
        }
        return null;
    }
}
