package yier.bubu.redis.integration.benchmark;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.redis.RedisBenchmarkCase;
import yier.bubu.redis.app.bench.redis.RedisBenchmarkCatalog;
import yier.bubu.redis.integration.command.DefaultCommandRegistrationTest;

import java.util.List;
import java.util.Set;

public class RedisBenchmarkCatalogCoverageGuardTest {
    @Test
    public void realServerGuardHasSixtySecondDeadline() throws NoSuchMethodException {
        Test annotation = RedisBenchmarkRealServerTest.class
                .getMethod("allOfficialCasesRunOrReportUnsupportedAgainstRealYierdis")
                .getAnnotation(Test.class);

        Assert.assertNotNull(annotation);
        Assert.assertEquals(60_000L, annotation.timeout());
    }

    @Test
    public void supportDeclarationsMatchDefaultCommandRegistration() {
        Set<String> commands = DefaultCommandRegistrationTest.defaultCommandNames();
        List<RedisBenchmarkCase> cases = new RedisBenchmarkCatalog().allCases();

        Assert.assertEquals(21, cases.size());
        for (RedisBenchmarkCase testCase : cases) {
            String message = "title=" + testCase.title()
                    + ", id=" + testCase.id()
                    + ", required=" + testCase.requiredCommands();
            Assert.assertEquals(
                    message,
                    testCase.support().supported(),
                    commands.containsAll(testCase.requiredCommands())
            );
        }
    }

    @Test
    public void inlinePingRequiresRegisteredPing() {
        RedisBenchmarkCase inlinePing = new RedisBenchmarkCatalog().caseById("ping_inline");
        Set<String> commands = DefaultCommandRegistrationTest.defaultCommandNames();

        Assert.assertEquals(Set.of("PING"), inlinePing.requiredCommands());
        Assert.assertTrue(commands.contains("PING"));
    }

    @Test
    public void unsupportedCasesFollowCanonicalOrderAndRequireMissingCommands() {
        Set<String> commands = DefaultCommandRegistrationTest.defaultCommandNames();
        List<RedisBenchmarkCase> unsupportedCases = new RedisBenchmarkCatalog().allCases().stream()
                .filter(testCase -> !testCase.support().supported())
                .toList();

        Assert.assertEquals(
                List.of("spop", "zpopmin", "mset", "xadd"),
                unsupportedCases.stream().map(RedisBenchmarkCase::id).toList()
        );
        for (RedisBenchmarkCase testCase : unsupportedCases) {
            String message = "unsupported case must require a missing registered command: id="
                    + testCase.id() + ", required=" + testCase.requiredCommands();
            Assert.assertFalse(message, testCase.requiredCommands().isEmpty());
            Assert.assertTrue(
                    message,
                    testCase.requiredCommands().stream().anyMatch(command -> !commands.contains(command))
            );
        }
    }

    @Test
    public void defaultCommandNamesAreUnmodifiable() {
        Set<String> commands = DefaultCommandRegistrationTest.defaultCommandNames();

        Assert.assertTrue(commands.contains("PING"));
        Assert.assertThrows(UnsupportedOperationException.class, () -> commands.remove("PING"));
        Assert.assertTrue(commands.contains("PING"));
    }
}
