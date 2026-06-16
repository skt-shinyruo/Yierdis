package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SuiteProfileFactoryTest {
    @Test
    public void releaseProfileHasStableCoreAndRiskScenarios() {
        List<ScenarioDefinition> scenarios = SuiteProfileFactory.expand(SuiteProfileName.RELEASE);

        assertHasScenario(scenarios, "release-ping-latency");
        assertHasScenario(scenarios, "release-set-get-256b-c64-p8");
        assertHasScenario(scenarios, "release-append-256b-c64-p8");
        assertHasScenario(scenarios, "release-hll-sparse-c64-p8");
        assertHasScenario(scenarios, "release-hll-dense-c64-p8");
        assertHasScenario(scenarios, "release-native-defrag-append");
        assertHasScenario(scenarios, "release-maxmemory-eviction");
        assertHasScenario(scenarios, "release-ttl-expiration");

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
        for (ScenarioDefinition releaseScenario : release) {
            assertHasScenario(full, releaseScenario.id());
        }

        assertHasScenario(full, "full-list-lpush");
        assertHasScenario(full, "full-hash-hset");
        assertHasScenario(full, "full-set-sadd");
        assertHasScenario(full, "full-zset-zadd");
        assertHasScenario(full, "full-scan-count-100");
        assertHasScenario(full, "full-mixed-read-write-hot");
    }

    @Test
    public void scenarioIdsAreUniqueAndUseStableLowercaseNames() {
        for (SuiteProfileName profile : SuiteProfileName.values()) {
            Set<String> ids = new HashSet<>();
            for (ScenarioDefinition scenario : SuiteProfileFactory.expand(profile)) {
                Assert.assertTrue("duplicate id " + scenario.id(), ids.add(scenario.id()));
                Assert.assertTrue("id must be lowercase kebab: " + scenario.id(), scenario.id().matches("[a-z0-9-]+"));
                Assert.assertTrue("requests must be positive", scenario.requests() > 0);
                Assert.assertTrue("keyspace must be positive", scenario.keyspace() > 0);
                Assert.assertTrue("clients must be positive", scenario.clients() > 0);
                Assert.assertTrue("pipeline must be positive", scenario.pipeline() > 0);
            }
        }
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

    private static void assertHasScenario(List<ScenarioDefinition> scenarios, String id) {
        scenario(scenarios, id);
    }
}
