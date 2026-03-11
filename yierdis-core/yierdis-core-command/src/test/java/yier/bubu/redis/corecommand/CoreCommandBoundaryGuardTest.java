package yier.bubu.redis.corecommand;

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
                                if (trimmed.startsWith("import yier.bubu.redis.db.")
                                        && !trimmed.startsWith("import yier.bubu.redis.db.memory.api.")) {
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

