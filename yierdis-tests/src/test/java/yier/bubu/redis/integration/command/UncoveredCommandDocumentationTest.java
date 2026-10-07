package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class UncoveredCommandDocumentationTest {
    @Test
    public void unregisteredCommonCommandsAreNamedInTheUncoveredCommandSection() throws IOException {
        List<String> candidates = List.of(
                "LLEN", "HSTRLEN", "ZMSCORE", "FLUSHALL", "SORT", "XADD", "WATCH", "UNWATCH"
        );
        String section = uncoveredCommandSection(Files.readString(protocolReference(), StandardCharsets.UTF_8));
        Set<String> registered = DefaultCommandRegistrationTest.defaultCommandNames();
        List<String> missingFromDoc = new ArrayList<>();
        for (String name : candidates) {
            if (!registered.contains(name) && !section.contains("`" + name + "`")) {
                missingFromDoc.add(name);
            }
        }
        Assert.assertEquals("unregistered but not documented", List.of(), missingFromDoc);
    }

    private static String uncoveredCommandSection(String doc) {
        String heading = "### 当前未覆盖的命令";
        int start = doc.indexOf(heading);
        Assert.assertTrue("missing section " + heading, start >= 0);
        int end = doc.indexOf("\n## ", start);
        Assert.assertTrue("section has no following heading", end > start);
        return doc.substring(start, end);
    }

    private static Path protocolReference() {
        // Maven Surefire 在 yierdis-tests 目录启动，协议文档在仓库根；从仓库根启动时走第二条路径。
        Path cwd = Path.of("").toAbsolutePath().normalize();
        Path fromModule = cwd.resolve("../docs/project-docs/protocol-reference.md").normalize();
        if (Files.isRegularFile(fromModule)) {
            return fromModule;
        }
        Path fromRepo = cwd.resolve("docs/project-docs/protocol-reference.md");
        Assert.assertTrue("cannot locate protocol-reference.md from " + cwd, Files.isRegularFile(fromRepo));
        return fromRepo;
    }
}
