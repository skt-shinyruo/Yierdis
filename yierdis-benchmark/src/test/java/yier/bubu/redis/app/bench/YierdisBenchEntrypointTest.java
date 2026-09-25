package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.redis.RedisBenchmarkOptions;
import yier.bubu.redis.app.bench.storage.StorageBenchmarkOptions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class YierdisBenchEntrypointTest {
    private static final List<String> REDIS_OPTIONS = List.of(
            "--host",
            "--port",
            "--requests",
            "--clients",
            "--data-size",
            "--pipeline",
            "--keyspace",
            "--keep-alive",
            "--tests",
            "--precision",
            "--seed",
            "--format",
            "--database"
    );

    private static final List<String> STORAGE_OPTIONS = List.of(
            "--keys",
            "--key-size",
            "--value-size",
            "--warmup-operations",
            "--precision",
            "--format"
    );

    @Test
    public void redisCommandExposesTheReplacementOptions() {
        Assert.assertTrue(
                "missing replacement options",
                RedisBenchmarkOptions.OPTION_NAMES.containsAll(REDIS_OPTIONS)
        );
    }

    @Test
    public void storageCommandExposesItsFootprintOptions() {
        Assert.assertTrue(
                "missing options",
                StorageBenchmarkOptions.OPTION_NAMES.containsAll(STORAGE_OPTIONS)
        );
    }

    /** 这个 jar 是给人用的工具，但不提供 --help：参数错误只打一行原因，退出码 2，不打 usage。 */
    @Test
    public void helpFlagIsRejectedWithoutUsageAtEveryLevel() {
        for (String[] argv : List.of(new String[]{"--help"}, new String[]{"storage", "--help"})) {
            StringWriter out = new StringWriter();
            StringWriter err = new StringWriter();

            int exitCode = YierdisBench.execute(argv, new PrintWriter(out), new PrintWriter(err));

            String label = String.join(" ", argv);
            Assert.assertEquals(label, 2, exitCode);
            Assert.assertEquals(label, "", out.toString());
            Assert.assertTrue(label + ": " + err, err.toString().contains("--help"));
            Assert.assertFalse(label + ": " + err, err.toString().contains("Usage"));
        }
    }
}
