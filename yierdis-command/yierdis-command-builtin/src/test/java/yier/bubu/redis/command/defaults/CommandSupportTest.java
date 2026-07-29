package yier.bubu.redis.command.defaults;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

public class CommandSupportTest {
    private static final YierdisDbRouter TEST_ROUTER = new YierdisDbRouter() {
        @Override
        public DbEngine dbFor(DbIndexSession session) {
            return null;
        }

        @Override
        public int databases() {
            return 1;
        }
    };

    @Test
    public void utf8PrefersReadOnlyFastPathWhenAvailable() {
        ExecutionRequest request = fastPathAwareRequest(ascii("PING"));

        Assert.assertEquals("PING", CommandSupport.utf8(ArgReader.of(request).bytes(0)));
    }

    @Test
    public void parseLongPrefersReadOnlyFastPathWhenAvailable() {
        ExecutionRequest request = fastPathAwareRequest(ascii("12345"));

        Assert.assertEquals(12345L, ArgReader.of(request).longAt(0));
    }

    @Test
    public void sliceResetFromRequestPrefersReadOnlyFastPathWhenAvailable() {
        byte[] first = ascii("k1");
        byte[] second = ascii("k2");
        CommandSupport support = new CommandSupport(TEST_ROUTER, null);
        ExecutionRequest request = fastPathAwareRequest(ascii("DEL"), first, second);

        support.sliceResetFromRequest(request, 1, 2);

        Assert.assertSame(first, support.slice().get(0));
        Assert.assertSame(second, support.slice().get(1));
    }

    @Test
    public void preparedMutationRendersItsActionResultAndClosesTheMutation() {
        TrackingMutation mutation = new TrackingMutation(true);
        AtomicInteger actions = new AtomicInteger();
        PreparedCommand prepared = CommandSupport.preparedMutation(
                ReplyShapes.simpleString("OK"), mutation, context -> {
                    actions.incrementAndGet();
                    return CommandResult.reply(RedisReplies.simpleString("OK"));
                });

        Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
        Assert.assertEquals(List.of("simple:OK"), render(prepared));
        Assert.assertEquals(1, actions.get());
        Assert.assertEquals(0, mutation.commitCount());
        prepared.close();
        prepared.close();
        Assert.assertEquals(1, mutation.closeCount());
    }

    @Test
    public void preparedMutationTranslatesOnlyExpectedStorageFailures() {
        assertControlError(new WrongTypeException(),
                "control:WRONGTYPE Operation against a key holding the wrong kind of value");
        assertControlError(new YierdisCommandException("ERR semantic failure"),
                "control:ERR semantic failure");
    }

    @Test
    public void preparedMutationLeavesIllegalArgumentExceptionUntouched() {
        TrackingMutation mutation = new TrackingMutation(true);
        IllegalArgumentException failure = new IllegalArgumentException("programming failure");
        PreparedCommand prepared = CommandSupport.preparedMutation(
                ReplyShapes.maximum(), mutation, (Function<CommandExecutionContext, CommandResult>) context -> {
                    throw failure;
                });

        Assert.assertSame(failure, Assert.assertThrows(IllegalArgumentException.class, () -> render(prepared)));
        prepared.close();
        Assert.assertEquals(1, mutation.closeCount());
    }

    private static void assertControlError(RuntimeException failure, String expectedEvent) {
        TrackingMutation mutation = new TrackingMutation(true);
        PreparedCommand prepared = CommandSupport.preparedMutation(
                ReplyShapes.maximum(), mutation, (Function<CommandExecutionContext, CommandResult>) context -> {
                    throw failure;
                });

        Assert.assertEquals(List.of(expectedEvent), render(prepared));
        prepared.close();
    }

    private static List<String> render(PreparedCommand prepared) {
        java.util.ArrayList<String> events = new java.util.ArrayList<>();
        RedisReplyWriter writer = (RedisReplyWriter) Proxy.newProxyInstance(
                RedisReplyWriter.class.getClassLoader(), new Class<?>[]{RedisReplyWriter.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return handleObjectMethod(proxy, method.getName(), args);
                    }
                    if (method.getName().equals("simpleString")) {
                        events.add("simple:" + args[0]);
                        return null;
                    }
                    if (method.getName().equals("controlError")) {
                        events.add("control:" + args[0]);
                        return null;
                    }
                    throw new UnsupportedOperationException("unexpected method: " + method.getName());
                });
        try (ByteArrayExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("TEST", List.of());
             CommandExecutionContext context = CommandExecutionContext.forRequest(session(), writer, request)) {
            prepared.execute(context);
        }
        return events;
    }

    private static CommandSession session() {
        return (CommandSession) Proxy.newProxyInstance(
                CommandSession.class.getClassLoader(), new Class<?>[]{CommandSession.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return handleObjectMethod(proxy, method.getName(), args);
                    }
                    throw new UnsupportedOperationException("unexpected method: " + method.getName());
                });
    }

    private static ExecutionRequest fastPathAwareRequest(byte[]... argv) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return handleObjectMethod(proxy, method.getName(), args);
            }
            return switch (method.getName()) {
                case "argc" -> argv.length;
                case "isNull" -> argv[(int) args[0]] == null;
                case "len" -> {
                    byte[] arg = argv[(int) args[0]];
                    yield arg == null ? -1 : arg.length;
                }
                case "byteAt" -> argv[(int) args[0]][(int) args[1]];
                case "copyToByteArray" -> {
                    byte[] arg = argv[(int) args[0]];
                    System.arraycopy(arg, 0, (byte[]) args[1], (int) args[2], arg.length);
                    yield null;
                }
                case "toByteArray" -> throw new AssertionError("unexpected defensive copy");
                case "readOnlyByteArray" -> argv[(int) args[0]];
                case "retainedBytes" -> retainedBytes(argv);
                case "close" -> null;
                default -> throw new UnsupportedOperationException("unexpected method: " + method.getName());
            };
        };
        return (ExecutionRequest) Proxy.newProxyInstance(
                ExecutionRequest.class.getClassLoader(),
                new Class[]{ExecutionRequest.class},
                handler
        );
    }

    private static Object handleObjectMethod(Object proxy, String name, Object[] args) {
        if ("toString".equals(name)) {
            return proxy.getClass().getName();
        }
        if ("hashCode".equals(name)) {
            return System.identityHashCode(proxy);
        }
        if ("equals".equals(name)) {
            return proxy == args[0];
        }
        throw new UnsupportedOperationException("unexpected Object method: " + name);
    }

    private static int retainedBytes(byte[][] argv) {
        int retained = 0;
        for (byte[] arg : argv) {
            retained += arg == null ? 0 : arg.length;
        }
        return retained;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class TrackingMutation implements PreparedMutation<String> {
        private final boolean current;
        private int closeCount;
        private int commitCount;

        private TrackingMutation(boolean current) {
            this.current = current;
        }

        int closeCount() {
            return closeCount;
        }

        int commitCount() {
            return commitCount;
        }

        @Override
        public String preview() {
            return "preview";
        }

        @Override
        public boolean isCurrent() {
            return current;
        }

        @Override
        public MutationOutcome commit(yier.bubu.redis.common.command.MutationContext context) {
            commitCount++;
            return MutationOutcome.NONE;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
