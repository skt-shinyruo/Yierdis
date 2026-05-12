package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class OperationCoverageMatrixTest {
    private static final Path MATRIX = Path.of("docs/project-docs/operation-test-coverage-matrix.md");
    private static final Pattern COMMAND_HEADING = Pattern.compile("(?m)^### ([A-Z][A-Z0-9-]*)$");
    private static final Pattern STATUS_LINE = Pattern.compile(
            "^- \\*\\*(Command layer|DB API|Native internals)\\*\\*: `([^`]+)` - .+$"
    );
    private static final Set<String> VALID_STATUSES = Set.of(
            "covered",
            "covered-by-shared-test",
            "missing",
            "not-applicable"
    );

    @Test
    public void matrixContainsEveryRegisteredDefaultCommand() {
        String matrix = readMatrix();
        Set<String> headings = commandHeadings(matrix);

        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            CommandRegistry registry = registryOf(processor);
            for (String upperName : registry.upperNamesSorted()) {
                Assert.assertTrue(
                        "missing matrix entry for registered command " + upperName,
                        headings.contains(upperName)
                );
            }
        });
    }

    @Test
    public void matrixRowsUseKnownStatusVocabulary() {
        List<String> invalid = invalidStatusLines(readMatrix());
        Assert.assertTrue("invalid matrix status lines: " + invalid, invalid.isEmpty());
    }

    @Test
    public void stringAndBitmapRowsStartAsConcreteTemplate() {
        String matrix = readMatrix();

        assertRowStatus(matrix, "SET", "Command layer", "covered");
        assertRowStatus(matrix, "GET", "Command layer", "covered");
        assertRowStatus(matrix, "STRLEN", "Command layer", "covered");
        assertRowStatus(matrix, "APPEND", "Command layer", "covered");
        assertRowStatus(matrix, "SETBIT", "Command layer", "covered");
        assertRowStatus(matrix, "GETBIT", "Command layer", "covered");
        assertRowStatus(matrix, "BITCOUNT", "Command layer", "covered");
        assertRowStatus(matrix, "INCR", "Command layer", "covered");
        assertRowStatus(matrix, "DECR", "Command layer", "covered");
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

    private static List<String> invalidStatusLines(String matrix) {
        ArrayList<String> invalid = new ArrayList<>();
        String[] lines = matrix.split("\\R");
        for (String line : lines) {
            if (!line.startsWith("- **Command layer**:")
                    && !line.startsWith("- **DB API**:")
                    && !line.startsWith("- **Native internals**:")) {
                continue;
            }

            Matcher matcher = STATUS_LINE.matcher(line);
            if (!matcher.matches() || !VALID_STATUSES.contains(matcher.group(2))) {
                invalid.add(line);
            }
        }
        return invalid;
    }

    private static void assertRowStatus(String matrix, String command, String layer, String expectedStatus) {
        Pattern sectionPattern = Pattern.compile(
                "(?ms)^### " + Pattern.quote(command) + "$(?<section>.*?)(?=^### |\\z)"
        );
        Matcher sectionMatcher = sectionPattern.matcher(matrix);
        Assert.assertTrue("missing matrix section for " + command, sectionMatcher.find());

        Pattern row = Pattern.compile(
                "(?m)^- \\*\\*" + Pattern.quote(layer) + "\\*\\*: `([^`]+)` - .+$"
        );
        Matcher matcher = row.matcher(sectionMatcher.group("section"));
        Assert.assertTrue("missing " + layer + " row for " + command, matcher.find());
        Assert.assertEquals(command + " " + layer, expectedStatus, matcher.group(1));
    }

    private static CommandRegistry registryOf(YierdisFastCommandProcessor processor) {
        try {
            Field field = YierdisFastCommandProcessor.class.getDeclaredField("registry");
            field.setAccessible(true);
            return (CommandRegistry) field.get(processor);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access command processor registry", e);
        }
    }
}
