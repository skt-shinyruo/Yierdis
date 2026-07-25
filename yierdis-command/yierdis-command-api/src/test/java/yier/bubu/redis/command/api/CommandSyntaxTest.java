package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;

public class CommandSyntaxTest {
    @Test
    public void arityValidatesAndEmitsMetadataFromOneObject() {
        CommandArity exact = CommandArity.exact(2);
        Assert.assertEquals(2, exact.redisMetadataArity());
        Assert.assertNull(exact.validate("get", ArgReader.of(request("GET", "k"))));
        Assert.assertNotNull(exact.validate("get", ArgReader.of(request("GET"))));

        CommandArity oneOf = CommandArity.oneOf(2, 4);
        Assert.assertEquals(-2, oneOf.redisMetadataArity());
        Assert.assertNull(oneOf.validate("bitcount", ArgReader.of(request("BITCOUNT", "k"))));
        Assert.assertNull(oneOf.validate("bitcount", ArgReader.of(request("BITCOUNT", "k", "0", "1"))));
    }

    @Test
    public void registrationTakesOneDefinitionWithoutASeparateName() throws Exception {
        Assert.assertNotNull(CommandModule.Registration.class.getMethod(
                "register", CommandDefinition.class));
        for (java.lang.reflect.Method method : CommandModule.Registration.class.getMethods()) {
            if (method.getName().equals("register")) {
                Assert.assertArrayEquals(new Class<?>[]{CommandDefinition.class}, method.getParameterTypes());
            }
        }
    }

    @Test
    public void commandParsersExposeOnlyTheTwoNonArityFactories() {
        java.util.List<String> publicStaticSignatures = java.util.Arrays.stream(
                        CommandParsers.class.getDeclaredMethods()
                )
                .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
                .filter(method -> java.lang.reflect.Modifier.isStatic(method.getModifiers()))
                .map(method -> method.getName() + java.util.Arrays.toString(method.getParameterTypes()))
                .sorted()
                .toList();

        Assert.assertEquals(java.util.List.of("args[]", "request[]"), publicStaticSignatures);
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], java.util.List.of(args).subList(1, args.length));
    }
}
