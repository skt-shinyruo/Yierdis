package yier.bubu.redis.architecture;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ArchitecturePolicyResourceTest {
    @Test
    public void architecturePolicyResourceNamesCurrentBoundaryRules() throws Exception {
        try (InputStream in = ArchitecturePolicyResourceTest.class.getResourceAsStream("/architecture-policy.yml")) {
            Assert.assertNotNull("missing architecture-policy.yml", in);
            String policy = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertTrue(policy.contains("yierdis-core-command:"));
            Assert.assertTrue(policy.contains("yierdis-executor-core:"));
            Assert.assertTrue(policy.contains("yierdis-storage-api:"));
            Assert.assertTrue(policy.contains("yierdis-runtime-api:"));
            Assert.assertTrue(policy.contains("yierdis-core-api:"));
            Assert.assertTrue(policy.contains("yierdis-server:"));
            Assert.assertTrue(policy.contains("forbidden_imports:"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.protocol.reply"));
        }
    }
}
