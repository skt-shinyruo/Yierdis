package yier.bubu.redis.protocol;

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

public class ReplySsoTGuardTest {
    @Test
    public void serverAndCoreProductionCodeMustNotReferenceReplyValue() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForProtocolReplyModelAuthorityLeaks(
                workspaceRoot,
                workspaceRoot.resolve("yierdis-app/yierdis-server-app/src/main/java"),
                offenders
        );
        scanned += scanCoreModulesForProtocolReplyModelAuthorityLeaks(
                workspaceRoot,
                offenders
        );

        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server/core 生产代码重新引用 protocol reply model，可能将其作为 ReplyWriter 之外的回包语义 authority：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverAndCoreProductionCodeMustNotUseReplyValueEncoderAuthorityHelpers() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForEncoderAuthorityHelperLeaks(
                workspaceRoot,
                workspaceRoot.resolve("yierdis-app/yierdis-server-app/src/main/java"),
                offenders
        );
        scanned += scanCoreModulesForEncoderAuthorityHelperLeaks(
                workspaceRoot,
                offenders
        );

        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server/core 生产代码重新使用 ReplyValue encoder helper 作为 ReplyWriter 之外的回包 authority：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverProductionCodeMustNotConstructProtocolReplyValuesDirectly() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenTexts(
                workspaceRoot,
                workspaceRoot.resolve("yierdis-app/yierdis-server-app/src/main/java"),
                offenders,
                "ReplyValue.",
                "ReplyArray(",
                "ReplyMap("
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server 生产代码重新直接构造 protocol reply model，可能绕开 ReplyWriter 语义 authority：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void protocolRequestDocumentationMustStayDtoOnly() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        Path requestFile = workspaceRoot.resolve(
                "yierdis-protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1Request.java"
        );
        Assert.assertTrue("缺少 CustomProtocolV1Request.java", Files.isRegularFile(requestFile));

        String requestSource = Files.readString(requestFile, StandardCharsets.UTF_8);
        Assert.assertTrue("request model 必须声明 protocol DTO 边界", requestSource.contains("This is a protocol-layer DTO only"));
    }

    @Test
    public void replyValueDocumentationMustNotDescribeCommandLayerReplyIr() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        Path replyValueFile = workspaceRoot.resolve(
                "yierdis-protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/reply/ReplyValue.java"
        );
        Assert.assertTrue("缺少 ReplyValue.java", Files.isRegularFile(replyValueFile));

        String source = Files.readString(replyValueFile, StandardCharsets.UTF_8);
        Assert.assertFalse("ReplyValue 不应再被描述为 reply IR", source.contains("Reply IR"));
        Assert.assertFalse("ReplyValue 不应再被描述为命令层回包语义模型", source.contains("命令层回包语义"));
        Assert.assertFalse("ReplyValue 不应再被描述为 command-layer reply model", source.contains("command-layer reply model"));
    }

    @Test
    public void protocolReplyModelDocumentationMustNotUseIrFraming() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        Path pomFile = workspaceRoot.resolve("yierdis-protocol/yierdis-custom-v1-wire/pom.xml");
        Assert.assertTrue("缺少 yierdis-custom-v1-wire/pom.xml", Files.isRegularFile(pomFile));

        String pom = Files.readString(pomFile, StandardCharsets.UTF_8);
        Assert.assertFalse("custom-v1-wire 模块描述不应再使用 Reply IR wording", pom.contains("Reply IR model"));

        Path replyPackage = workspaceRoot.resolve(
                "yierdis-protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/reply"
        );
        Assert.assertTrue("缺少 protocol reply package", Files.isDirectory(replyPackage));

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenTexts(
                workspaceRoot,
                replyPackage,
                offenders,
                "IR array 值",
                "IR boolean 值",
                "IR bytes 值",
                "IR double 值",
                "IR error 值",
                "IR integer 值",
                "IR map 值",
                "IR null 值",
                "IR string 值"
        );
        Assert.assertTrue("未扫描到任何 reply model Java 文件", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail("检测到 protocol reply model 仍使用 IR framing：\n" + String.join("\n", offenders));
        }
    }

    @Test
    public void ndjsonEncoderDocumentationMustNotClaimServerReplySsot() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        Path encoderFile = workspaceRoot.resolve(
                "yierdis-protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java"
        );
        Assert.assertTrue("缺少 CustomProtocolV1NdjsonEncoder.java", Files.isRegularFile(encoderFile));

        String source = Files.readString(encoderFile, StandardCharsets.UTF_8);
        Assert.assertFalse("NDJSON encoder 不应再被描述为 server reply SSOT", source.contains("编码器（SSOT）"));
        Assert.assertFalse("NDJSON encoder 不应再被描述为 server reply authority", source.contains("server reply authority"));
    }

    private static int scanCoreModulesForForbiddenText(Path workspaceRoot, List<String> offenders, String forbiddenText)
            throws IOException {
        Path coreRoot = workspaceRoot.resolve("yierdis-core");
        Assert.assertTrue("缺少 yierdis-core 模块目录", Files.isDirectory(coreRoot));

        int scanned = 0;
        try (Stream<Path> modules = Files.list(coreRoot)) {
            List<Path> moduleRoots = modules
                    .filter(Files::isDirectory)
                    .map(module -> module.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .toList();
            for (Path sourceRoot : moduleRoots) {
                scanned += scanForForbiddenText(workspaceRoot, sourceRoot, offenders, forbiddenText);
            }
        }
        return scanned;
    }

    private static int scanCoreModulesForForbiddenTexts(Path workspaceRoot, List<String> offenders, String... forbiddenTexts)
            throws IOException {
        Path coreRoot = workspaceRoot.resolve("yierdis-core");
        Assert.assertTrue("缺少 yierdis-core 模块目录", Files.isDirectory(coreRoot));

        int scanned = 0;
        try (Stream<Path> modules = Files.list(coreRoot)) {
            List<Path> moduleRoots = modules
                    .filter(Files::isDirectory)
                    .map(module -> module.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .toList();
            for (Path sourceRoot : moduleRoots) {
                scanned += scanForForbiddenTexts(workspaceRoot, sourceRoot, offenders, forbiddenTexts);
            }
        }
        return scanned;
    }

    private static int scanCoreModulesForProtocolReplyModelAuthorityLeaks(Path workspaceRoot, List<String> offenders)
            throws IOException {
        Path coreRoot = workspaceRoot.resolve("yierdis-core");
        Assert.assertTrue("缺少 yierdis-core 模块目录", Files.isDirectory(coreRoot));

        int scanned = 0;
        try (Stream<Path> modules = Files.list(coreRoot)) {
            List<Path> moduleRoots = modules
                    .filter(Files::isDirectory)
                    .map(module -> module.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .toList();
            for (Path sourceRoot : moduleRoots) {
                scanned += scanForProtocolReplyModelAuthorityLeaks(workspaceRoot, sourceRoot, offenders);
            }
        }
        return scanned;
    }

    private static int scanCoreModulesForEncoderAuthorityHelperLeaks(Path workspaceRoot, List<String> offenders)
            throws IOException {
        Path coreRoot = workspaceRoot.resolve("yierdis-core");
        Assert.assertTrue("缺少 yierdis-core 模块目录", Files.isDirectory(coreRoot));

        int scanned = 0;
        try (Stream<Path> modules = Files.list(coreRoot)) {
            List<Path> moduleRoots = modules
                    .filter(Files::isDirectory)
                    .map(module -> module.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .toList();
            for (Path sourceRoot : moduleRoots) {
                scanned += scanForEncoderAuthorityHelperLeaks(workspaceRoot, sourceRoot, offenders);
            }
        }
        return scanned;
    }

    private static int scanForForbiddenText(Path workspaceRoot, Path sourceRoot, List<String> offenders, String forbiddenText)
            throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return 0;
        }

        int[] scanned = {0};
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".java"))
                    .forEach(path -> {
                        scanned[0]++;
                        try {
                            scanFileForForbiddenText(workspaceRoot, path, offenders, forbiddenText);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanForForbiddenTexts(Path workspaceRoot, Path sourceRoot, List<String> offenders, String... forbiddenTexts)
            throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return 0;
        }

        int[] scanned = {0};
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".java"))
                    .forEach(path -> {
                        scanned[0]++;
                        try {
                            scanFileForForbiddenTexts(workspaceRoot, path, offenders, forbiddenTexts);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanForProtocolReplyModelAuthorityLeaks(Path workspaceRoot, Path sourceRoot, List<String> offenders)
            throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return 0;
        }

        int[] scanned = {0};
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".java"))
                    .forEach(path -> {
                        scanned[0]++;
                        try {
                            scanFileForProtocolReplyModelAuthorityLeaks(workspaceRoot, path, offenders);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanForEncoderAuthorityHelperLeaks(Path workspaceRoot, Path sourceRoot, List<String> offenders)
            throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return 0;
        }

        int[] scanned = {0};
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".java"))
                    .forEach(path -> {
                        scanned[0]++;
                        try {
                            scanFileForEncoderAuthorityHelperLeaks(workspaceRoot, path, offenders);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static void scanFileForForbiddenText(Path workspaceRoot, Path file, List<String> offenders, String forbiddenText)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains(forbiddenText)) {
                offenders.add(relativePath(workspaceRoot, file) + ":" + (i + 1) + " -> " + forbiddenText);
            }
        }
    }

    private static void scanFileForForbiddenTexts(Path workspaceRoot, Path file, List<String> offenders, String... forbiddenTexts)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            for (String forbiddenText : forbiddenTexts) {
                if (line.contains(forbiddenText)) {
                    offenders.add(relativePath(workspaceRoot, file) + ":" + (i + 1) + " -> " + forbiddenText);
                }
            }
        }
    }

    private static void scanFileForProtocolReplyModelAuthorityLeaks(Path workspaceRoot, Path file, List<String> offenders)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (!line.contains("yier.bubu.redis.protocol.reply.")) {
                continue;
            }
            if (line.contains("ReplyErrorKind") || line.contains("ReplyErrorSanitizer")) {
                continue;
            }
            offenders.add(relativePath(workspaceRoot, file) + ":" + (i + 1) + " -> protocol.reply authority");
        }
    }

    private static void scanFileForEncoderAuthorityHelperLeaks(Path workspaceRoot, Path file, List<String> offenders)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        boolean hasStaticImport = lines.stream().anyMatch(line ->
                line.contains("import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.writeOkEnvelope;")
                        || line.contains("import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.writeValue;")
                        || line.contains("import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.*;")
        );

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.contains("CustomProtocolV1NdjsonEncoder.writeOkEnvelope(")
                    || line.contains("CustomProtocolV1NdjsonEncoder.writeValue(")) {
                offenders.add(relativePath(workspaceRoot, file) + ":" + (i + 1) + " -> encoder authority helper");
                continue;
            }
            if (hasStaticImport && (line.contains("writeOkEnvelope(") || line.contains("writeValue("))) {
                offenders.add(relativePath(workspaceRoot, file) + ":" + (i + 1) + " -> encoder authority helper");
            }
        }
    }

    private static String relativePath(Path workspaceRoot, Path file) {
        return workspaceRoot.relativize(file).toString().replace('\\', '/');
    }

    private static Path resolveWorkspaceRoot() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path p = cwd;
        for (int i = 0; i < 8 && p != null; i++) {
            if (isWorkspaceRoot(p)) {
                return p;
            }
            p = p.getParent();
        }

        String basedir = System.getProperty("basedir");
        if (basedir != null && !basedir.isBlank()) {
            Path base = Paths.get(basedir).toAbsolutePath().normalize();
            Path current = base;
            for (int i = 0; i < 8 && current != null; i++) {
                if (isWorkspaceRoot(current)) {
                    return current;
                }
                current = current.getParent();
            }
        }
        return null;
    }

    private static boolean isWorkspaceRoot(Path path) {
        return Files.isRegularFile(path.resolve("README.md"))
                && Files.isDirectory(path.resolve("yierdis-core"))
                && Files.isDirectory(path.resolve("yierdis-app/yierdis-server-app/src/main/java"))
                && Files.isDirectory(path.resolve("yierdis-protocol"));
    }
}
