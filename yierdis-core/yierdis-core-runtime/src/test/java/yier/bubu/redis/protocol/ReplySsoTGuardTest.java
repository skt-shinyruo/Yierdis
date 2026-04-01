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
        scanned += scanForForbiddenTexts(
                workspaceRoot,
                workspaceRoot.resolve("yierdis-server/src/main/java"),
                offenders,
                "import yier.bubu.redis.protocol.reply.ReplyValue;",
                "import yier.bubu.redis.protocol.reply.ReplyNull;",
                "import yier.bubu.redis.protocol.reply.ReplyBoolean;",
                "import yier.bubu.redis.protocol.reply.ReplyLong;",
                "import yier.bubu.redis.protocol.reply.ReplyDouble;",
                "import yier.bubu.redis.protocol.reply.ReplyString;",
                "import yier.bubu.redis.protocol.reply.ReplyBytes;",
                "import yier.bubu.redis.protocol.reply.ReplyArray;",
                "import yier.bubu.redis.protocol.reply.ReplyMap;",
                "import yier.bubu.redis.protocol.reply.ReplyError;"
        );
        scanned += scanCoreModulesForForbiddenTexts(
                workspaceRoot,
                offenders,
                "import yier.bubu.redis.protocol.reply.ReplyValue;",
                "import yier.bubu.redis.protocol.reply.ReplyNull;",
                "import yier.bubu.redis.protocol.reply.ReplyBoolean;",
                "import yier.bubu.redis.protocol.reply.ReplyLong;",
                "import yier.bubu.redis.protocol.reply.ReplyDouble;",
                "import yier.bubu.redis.protocol.reply.ReplyString;",
                "import yier.bubu.redis.protocol.reply.ReplyBytes;",
                "import yier.bubu.redis.protocol.reply.ReplyArray;",
                "import yier.bubu.redis.protocol.reply.ReplyMap;",
                "import yier.bubu.redis.protocol.reply.ReplyError;"
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
        scanned += scanForForbiddenTexts(
                workspaceRoot,
                workspaceRoot.resolve("yierdis-server/src/main/java"),
                offenders,
                "CustomProtocolV1NdjsonEncoder.writeOkEnvelope(",
                "CustomProtocolV1NdjsonEncoder.writeValue(",
                "import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.*;",
                "import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.writeOkEnvelope;",
                "import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.writeValue;",
                "writeOkEnvelope(",
                "writeValue("
        );
        scanned += scanCoreModulesForForbiddenTexts(
                workspaceRoot,
                offenders,
                "CustomProtocolV1NdjsonEncoder.writeOkEnvelope(",
                "CustomProtocolV1NdjsonEncoder.writeValue(",
                "import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.*;",
                "import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.writeOkEnvelope;",
                "import static yier.bubu.redis.protocol.v1.CustomProtocolV1NdjsonEncoder.writeValue;",
                "writeOkEnvelope(",
                "writeValue("
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
    public void replyValueDocumentationMustNotDescribeCommandLayerReplyIr() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        Path replyValueFile = workspaceRoot.resolve(
                "yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/reply/ReplyValue.java"
        );
        Assert.assertTrue("缺少 ReplyValue.java", Files.isRegularFile(replyValueFile));

        String source = Files.readString(replyValueFile, StandardCharsets.UTF_8);
        Assert.assertFalse("ReplyValue 不应再被描述为 reply IR", source.contains("Reply IR"));
        Assert.assertFalse("ReplyValue 不应再被描述为命令层回包语义模型", source.contains("命令层回包语义"));
    }

    @Test
    public void protocolReplyModelDocumentationMustNotUseIrFraming() throws IOException {
        Path workspaceRoot = resolveWorkspaceRoot();
        Assert.assertNotNull("无法定位仓库根目录", workspaceRoot);

        Path pomFile = workspaceRoot.resolve("yierdis-protocol/yierdis-protocol-model/pom.xml");
        Assert.assertTrue("缺少 yierdis-protocol-model/pom.xml", Files.isRegularFile(pomFile));

        String pom = Files.readString(pomFile, StandardCharsets.UTF_8);
        Assert.assertFalse("protocol-model 模块描述不应再使用 Reply IR wording", pom.contains("Reply IR model"));

        Path replyPackage = workspaceRoot.resolve(
                "yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/reply"
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
                "yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java"
        );
        Assert.assertTrue("缺少 CustomProtocolV1NdjsonEncoder.java", Files.isRegularFile(encoderFile));

        String source = Files.readString(encoderFile, StandardCharsets.UTF_8);
        Assert.assertFalse("NDJSON encoder 不应再被描述为 server reply SSOT", source.contains("编码器（SSOT）"));
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
                && Files.isDirectory(path.resolve("yierdis-server/src/main/java"))
                && Files.isDirectory(path.resolve("yierdis-protocol"));
    }
}
