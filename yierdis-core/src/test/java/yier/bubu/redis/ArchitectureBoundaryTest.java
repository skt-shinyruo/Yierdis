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
    public void dbAndOpsMustNotImportProtocolModel() throws IOException {
        List<String> offenders = new ArrayList<>();
        Path moduleRoot = resolveYierdisCoreModuleRoot();
        Assert.assertNotNull("无法定位 yierdis-core 模块根目录（未找到 src/main/java）", moduleRoot);

        int scanned = 0;
        scanned += scanForProtocolImports(moduleRoot.resolve("src/main/java/yier/bubu/redis/db"), offenders);
        scanned += scanForProtocolImports(moduleRoot.resolve("src/main/java/yier/bubu/redis/ops"), offenders);
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail("检测到协议层依赖泄漏（禁止 import yier.bubu.redis.protocol.*）：\n" + String.join("\n", offenders));
        }
    }

    private static int scanForProtocolImports(Path root, List<String> offenders) throws IOException {
        if (root == null || !Files.exists(root)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            String s = Files.readString(p, StandardCharsets.UTF_8);
                            if (s.contains("import yier.bubu.redis.protocol.")) {
                                offenders.add(p.toString());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static Path resolveYierdisCoreModuleRoot() {
        // Maven surefire 下通常为 yierdis-core 模块根目录；IDE/自定义运行环境下可能是仓库根目录。
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
        Path nested = base.resolve("yierdis-core");
        if (Files.isDirectory(nested.resolve("src/main/java"))) {
            return nested;
        }
        return null;
    }
}
