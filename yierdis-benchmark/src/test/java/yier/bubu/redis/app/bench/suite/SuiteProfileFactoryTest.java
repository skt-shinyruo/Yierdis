package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SuiteProfileFactoryTest {
    private static final List<String> RELEASE_IDS = List.of(
            "release-ping-latency",
            "release-set-get-128b-c32-p4",
            "release-set-get-256b-c64-p8",
            "release-set-get-1024b-c64-p8",
            "release-append-256b-c64-p8",
            "release-hll-sparse-c64-p8",
            "release-hll-dense-c64-p8",
            "release-hll-pfcount-c64-p8",
            "release-native-defrag-append",
            "release-maxmemory-eviction",
            "release-ttl-expiration"
    );
    private static final List<String> FULL_ONLY_IDS = List.of(
            "full-list-lpush",
            "full-hash-hset",
            "full-set-sadd",
            "full-zset-zadd",
            "full-scan-count-100",
            "full-mixed-read-write-hot"
    );

    @Test
    public void releaseProfileHasStableCoreAndRiskScenarios() {
        List<ScenarioDefinition> scenarios = SuiteProfileFactory.expand(SuiteProfileName.RELEASE);

        Assert.assertEquals(RELEASE_IDS, scenarioIds(scenarios));

        ScenarioDefinition setGet = scenario(scenarios, "release-set-get-256b-c64-p8");
        Assert.assertEquals(BenchWorkloadKind.SET_GET, setGet.workload());
        Assert.assertEquals(256, setGet.dataSize());
        Assert.assertEquals(64, setGet.clients());
        Assert.assertEquals(8, setGet.pipeline());
        Assert.assertTrue(setGet.repeatIterations() >= 3);
        Assert.assertTrue(setGet.warmupIterations() >= 1);
    }

    @Test
    public void fullProfileIncludesReleaseScenariosAndExtendedFamilies() {
        List<ScenarioDefinition> release = SuiteProfileFactory.expand(SuiteProfileName.RELEASE);
        List<ScenarioDefinition> full = SuiteProfileFactory.expand(SuiteProfileName.FULL);

        Assert.assertTrue(full.size() > release.size());
        Assert.assertEquals(RELEASE_IDS, scenarioIds(release));

        List<String> expectedFull = new ArrayList<>(RELEASE_IDS);
        expectedFull.addAll(FULL_ONLY_IDS);
        Assert.assertEquals(expectedFull, scenarioIds(full));
    }

    @Test
    public void scenarioIdsAreUniqueAndUseStableLowercaseNames() {
        for (SuiteProfileName profile : SuiteProfileName.values()) {
            Set<String> ids = new HashSet<>();
            for (ScenarioDefinition scenario : SuiteProfileFactory.expand(profile)) {
                Assert.assertTrue("duplicate id " + scenario.id(), ids.add(scenario.id()));
                Assert.assertTrue("id must be lowercase kebab: " + scenario.id(), scenario.id().matches("[a-z0-9]+(?:-[a-z0-9]+)*"));
                Assert.assertTrue("requests must be positive", scenario.requests() > 0);
                Assert.assertTrue("keyspace must be positive", scenario.keyspace() > 0);
                Assert.assertTrue("clients must be positive", scenario.clients() > 0);
                Assert.assertTrue("pipeline must be positive", scenario.pipeline() > 0);
            }
        }
    }

    @Test
    public void expandRejectsNullProfile() {
        try {
            SuiteProfileFactory.expand(null);
            Assert.fail("expected null profile rejection");
        } catch (NullPointerException e) {
            Assert.assertEquals("profile", e.getMessage());
        }
    }

    @Test
    public void expandReturnsUnmodifiableScenarioList() {
        List<ScenarioDefinition> scenarios = SuiteProfileFactory.expand(SuiteProfileName.RELEASE);

        try {
            scenarios.clear();
            Assert.fail("expected unmodifiable scenario list");
        } catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    @Test
    public void scenarioRejectsUnstableIds() {
        assertRejectsScenarioId(null);
        assertRejectsScenarioId("");
        assertRejectsScenarioId("-");
        assertRejectsScenarioId("bad-");
        assertRejectsScenarioId("bad--id");
        assertRejectsScenarioId("Bad-Id");
        assertRejectsScenarioId("bad_id");
    }

    @Test
    public void scenarioRejectsBlankDisplayName() {
        IllegalArgumentException blank = assertRejectsScenario("displayName", "valid-id", "");
        Assert.assertEquals("displayName must not be blank", blank.getMessage());

        IllegalArgumentException whitespace = assertRejectsScenario("displayName", "valid-id", "   ");
        Assert.assertEquals("displayName must not be blank", whitespace.getMessage());
    }

    private static ScenarioDefinition scenario(List<ScenarioDefinition> scenarios, String id) {
        for (ScenarioDefinition scenario : scenarios) {
            if (scenario.id().equals(id)) {
                return scenario;
            }
        }
        Assert.fail("missing scenario " + id);
        return null;
    }

    private static List<String> scenarioIds(List<ScenarioDefinition> scenarios) {
        List<String> ids = new ArrayList<>();
        for (ScenarioDefinition scenario : scenarios) {
            ids.add(scenario.id());
        }
        return ids;
    }

    private static void assertRejectsScenarioId(String id) {
        IllegalArgumentException e = assertRejectsScenario("scenario id", id, "Display name");
        Assert.assertEquals("scenario id must be lowercase kebab-case", e.getMessage());
    }

    private static IllegalArgumentException assertRejectsScenario(String messagePart, String id, String displayName) {
        try {
            new ScenarioDefinition(
                    id,
                    displayName,
                    BenchWorkloadKind.PING,
                    1,
                    0,
                    1,
                    1,
                    1,
                    0,
                    1,
                    true
            );
            Assert.fail("expected rejection containing " + messagePart);
            return null;
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
            return e;
        }
    }
}
