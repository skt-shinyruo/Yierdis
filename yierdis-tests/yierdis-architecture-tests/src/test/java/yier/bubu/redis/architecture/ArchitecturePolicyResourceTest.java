package yier.bubu.redis.architecture;

import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ArchitecturePolicyResourceTest {
    @Test
    public void architecturePolicyResourceNamesCurrentBoundaryRules() throws Exception {
        Map<String, Object> policy = loadPolicy();
        Map<String, Object> modules = mapValue(policy, "modules");
        Set<String> requiredModules = Set.of(
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-server-executor",
                "yierdis-db-api",
                "yierdis-db-memory",
                "yierdis-db-testkit",
                "yierdis-server-runtime-api",
                "yierdis-server-core",
                "yierdis-server-runtime",
                "yierdis-server-main",
                "yierdis-networking-resp",
                "yierdis-networking-netty"
        );
        Assert.assertTrue("missing module policies: " + missing(requiredModules, modules.keySet()),
                modules.keySet().containsAll(requiredModules));

        for (String module : requiredModules) {
            Map<String, Object> modulePolicy = mapValue(modules, module);
            Assert.assertTrue(module + " must declare allowed_dependencies",
                    modulePolicy.get("allowed_dependencies") instanceof List<?>);
            Assert.assertTrue(module + " must declare forbidden_imports",
                    modulePolicy.get("forbidden_imports") instanceof List<?>);
        }

        Map<String, Object> serverMain = mapValue(modules, "yierdis-server-main");
        Assert.assertTrue(listValue(serverMain, "source_ownership").contains("application_composition_root"));
        Assert.assertTrue(listValue(serverMain, "source_ownership").contains("no_storage_internal_imports"));
        Assert.assertTrue(containsScalar(policy, "yier.bubu.redis.protocol.resp"));
        Assert.assertFalse(containsScalar(policy, "yierdis-networking-custom-v1"));
        Assert.assertFalse(containsScalar(policy, "yierdis-networking-custom-v1-execution"));
        Assert.assertFalse(containsScalar(policy, "protocol.custom.v1"));
    }

    @Test
    public void architecturePolicyResourceDocumentsTargetPackageOwnership() throws Exception {
        Map<String, Object> policy = loadPolicy();
        Map<String, Object> targetPackages = mapValue(policy, "target_packages");
        Assert.assertTrue(listValue(mapValue(targetPackages, "app_server"), "owns")
                .contains("yier.bubu.redis.app.server"));
        Assert.assertTrue(listValue(mapValue(targetPackages, "execution"), "owns")
                .contains("yier.bubu.redis.execution.api"));
        Assert.assertTrue(listValue(mapValue(targetPackages, "command"), "owns")
                .contains("yier.bubu.redis.command.kernel"));
        Assert.assertTrue(listValue(mapValue(targetPackages, "storage"), "owns")
                .contains("yier.bubu.redis.storage.memory"));
        for (Map.Entry<String, Object> entry : targetPackages.entrySet()) {
            Map<String, Object> target = mapValue(targetPackages, entry.getKey());
            Assert.assertTrue(entry.getKey() + " must declare retired_from_active_source_tree",
                    target.get("retired_from_active_source_tree") instanceof List<?>);
        }
        Assert.assertFalse("legacy migration exemptions are forbidden",
                containsKey(policy, "legacy_allowed_during_migration"));
    }

    private static Map<String, Object> loadPolicy() throws Exception {
        try (InputStream in = ArchitecturePolicyResourceTest.class.getResourceAsStream("/architecture-policy.yml")) {
            Assert.assertNotNull("missing architecture-policy.yml", in);
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            Object document = new Yaml(new SafeConstructor(options)).load(in);
            Assert.assertTrue("architecture policy root must be a mapping", document instanceof Map<?, ?>);
            return stringObjectMap((Map<?, ?>) document, "architecture policy root");
        }
    }

    private static Map<String, Object> mapValue(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        Assert.assertTrue(key + " must be a mapping", value instanceof Map<?, ?>);
        return stringObjectMap((Map<?, ?>) value, key);
    }

    private static Map<String, Object> stringObjectMap(Map<?, ?> value, String location) {
        for (Object key : value.keySet()) {
            Assert.assertTrue(location + " contains a non-string key", key instanceof String);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) value;
        return result;
    }

    private static List<?> listValue(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        Assert.assertTrue(key + " must be a list", value instanceof List<?>);
        return (List<?>) value;
    }

    private static boolean containsKey(Object value, String expectedKey) {
        if (value instanceof Map<?, ?> map) {
            return map.containsKey(expectedKey)
                    || map.values().stream().anyMatch(item -> containsKey(item, expectedKey));
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().anyMatch(item -> containsKey(item, expectedKey));
        }
        return false;
    }

    private static boolean containsScalar(Object value, String expected) {
        if (expected.equals(value)) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(item -> containsScalar(item, expected));
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().anyMatch(item -> containsScalar(item, expected));
        }
        return false;
    }

    private static Set<String> missing(Set<String> required, Set<String> actual) {
        java.util.HashSet<String> missing = new java.util.HashSet<>(required);
        missing.removeAll(actual);
        return missing;
    }
}
