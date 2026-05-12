package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerOperationCoverageMatrixTest {
    private static final Path MATRIX = Path.of("docs/project-docs/operation-test-coverage-matrix.md");
    private static final Pattern COMMAND_HEADING = Pattern.compile("(?m)^### ([A-Z][A-Z0-9-]*)$");

    @Test
    public void matrixContainsEveryRegisteredServerCommand() {
        CommandRegistry registry = new CommandRegistry();
        new ServerCommandModule(new TestServerInfoProvider()).register(registry);

        Set<String> headings = commandHeadings(readMatrix());
        for (String upperName : registry.upperNamesSorted()) {
            Assert.assertTrue(
                    "missing matrix entry for registered server command " + upperName,
                    headings.contains(upperName)
            );
        }
    }

    private static String readMatrix() {
        Path matrix = findMatrix();
        Assert.assertTrue("missing coverage matrix file: " + MATRIX, Files.isRegularFile(matrix));
        try {
            return Files.readString(matrix, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("unable to read coverage matrix " + MATRIX, e);
        }
    }

    private static Path findMatrix() {
        Path cwd = Path.of("").toAbsolutePath();
        for (Path dir = cwd; dir != null; dir = dir.getParent()) {
            Path candidate = dir.resolve(MATRIX);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return MATRIX;
    }

    private static Set<String> commandHeadings(String matrix) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        Matcher matcher = COMMAND_HEADING.matcher(matrix);
        while (matcher.find()) {
            out.add(matcher.group(1));
        }
        return out;
    }

    private static final class TestServerInfoProvider implements ServerInfoProvider {
        @Override
        public void info(ExecutionRequest request, CommandContext ctx) {
            ctx.out().emptyArray();
        }

        @Override
        public void stats(ExecutionRequest request, CommandContext ctx) {
            ctx.out().emptyArray();
        }
    }
}
