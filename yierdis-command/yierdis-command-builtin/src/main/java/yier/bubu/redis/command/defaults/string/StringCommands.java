package yier.bubu.redis.command.defaults.string;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.result.ByteValue;

public final class StringCommands implements CommandModule {
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;
    private static final String INVALID_SET_EXPIRE = "ERR invalid expire time in 'set' command";
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public StringCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("SET", CommandArity.min(3)), this::parseSet, this::set));
        registration.register(new CommandDefinition<>(syntax("GET", CommandArity.exact(2)), CommandParsers.args(), this::get));
        registration.register(new CommandDefinition<>(syntax("STRLEN", CommandArity.exact(2)), CommandParsers.args(), this::strlen));
        registration.register(new CommandDefinition<>(syntax("APPEND", CommandArity.exact(3)), CommandParsers.args(), this::append));
        registration.register(new CommandDefinition<>(syntax("SETBIT", CommandArity.exact(4)), CommandParsers.args(), this::setbit));
        registration.register(new CommandDefinition<>(syntax("GETBIT", CommandArity.exact(3)), CommandParsers.args(), this::getbit));
        registration.register(new CommandDefinition<>(syntax("BITCOUNT", CommandArity.oneOf(2, 4)),
                CommandParsers.request(), this::bitcount));
        registration.register(new CommandDefinition<>(syntax("INCR", CommandArity.exact(2)), CommandParsers.args(),
                (args, context) -> incrBy(args, 1L)));
        registration.register(new CommandDefinition<>(syntax("DECR", CommandArity.exact(2)), CommandParsers.args(),
                (args, context) -> incrBy(args, -1L)));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private record SetArgs(
            ExecutionRequest request,
            byte[] key,
            int valueIndex,
            SetMode mode,
            ExpireOption expire,
            boolean getOld
    ) {
    }

    private CommandParseResult<SetArgs> parseSet(ArgReader args) {
        byte[] key = args.bytes(1);
        SetMode mode = SetMode.NORMAL;
        ExpireOption expire = null;
        boolean getOld = false;
        for (int index = 3; index < args.argc(); index++) {
            if (args.is(index, "NX")) {
                if (mode != SetMode.NORMAL) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                mode = SetMode.NX;
                continue;
            }
            if (args.is(index, "XX")) {
                if (mode != SetMode.NORMAL) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                mode = SetMode.XX;
                continue;
            }
            if (args.is(index, "GET")) {
                if (getOld) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                getOld = true;
                continue;
            }
            if (args.is(index, "KEEPTTL")) {
                if (expire != null) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                expire = ExpireOption.keepTtl();
                continue;
            }
            if (!args.is(index, "EX") && !args.is(index, "PX")
                    && !args.is(index, "EXAT") && !args.is(index, "PXAT")) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            if (expire != null || index + 1 >= args.argc()) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            String option = CommandSupport.utf8(args.bytes(index));
            long value;
            try {
                value = args.longAt(++index);
            } catch (IllegalArgumentException failure) {
                return CommandParseResult.error(CommandParseError.integerOutOfRange());
            }
            if (value <= 0L) {
                return CommandParseResult.error(CommandParseError.custom(INVALID_SET_EXPIRE));
            }
            if ("EX".equalsIgnoreCase(option)) {
                expire = ExpireOption.ex(value);
            } else if ("PX".equalsIgnoreCase(option)) {
                expire = ExpireOption.px(value);
            } else if ("EXAT".equalsIgnoreCase(option)) {
                long expireAtMillis;
                try {
                    expireAtMillis = Math.multiplyExact(value, 1000L);
                } catch (ArithmeticException ignored) {
                    expireAtMillis = Long.MAX_VALUE;
                }
                if (expireAtMillis <= System.currentTimeMillis()) {
                    return CommandParseResult.error(CommandParseError.custom(INVALID_SET_EXPIRE));
                }
                expire = ExpireOption.exAt(value);
            } else {
                if (value <= System.currentTimeMillis()) {
                    return CommandParseResult.error(CommandParseError.custom(INVALID_SET_EXPIRE));
                }
                expire = ExpireOption.pxAt(value);
            }
        }
        return CommandParseResult.ok(new SetArgs(args.request(), key, 2, mode, expire, getOld));
    }

    private PreparedCommand set(SetArgs args, CommandPreparationContext context) {
        PreparedMutation<StringWriteOps.SetStringValue> mutation = support.commandDb(context).writes().strings()
                .prepareSet(
                        args.key(),
                        support.argSlice(args.request(), args.valueIndex()),
                        args.mode(),
                        args.expire()
                );
        StringWriteOps.SetStringValue preview = mutation.preview();
        return new PreparedSet(mutation, preview, args.getOld(), setShape(preview, args.getOld()));
    }

    private static ReplyShape setShape(StringWriteOps.SetStringValue preview, boolean getOld) {
        if (!preview.applied()) {
            return ReplyShapes.nullValue();
        }
        if (!getOld) {
            return ReplyShapes.simpleString("OK");
        }
        ByteValue value = preview.oldValue();
        return value.isNull()
                ? ReplyShapes.nullValue()
                : ReplyShapes.bulkString(value.payloadLength(), value.retainedMemoryBytes());
    }

    private PreparedCommand get(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.byteValue(support.commandDb(context).reads().strings()
                .getStringValue(support.argView(request, 1)));
    }

    private PreparedCommand strlen(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        long length = support.commandDb(context).reads().strings().strlen(support.argView(request, 1));
        return CommandSupport.fixed(ReplyShapes.integer(length), execution -> execution.reply().integer(length));
    }

    private PreparedCommand append(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            long length = support.commandDb(execution).writes().strings()
                    .append(request.readOnlyByteArray(1), support.argSlice(request, 2)).value();
            execution.reply().integer(length);
        });
    }

    private PreparedCommand setbit(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        long offset = CommandSupport.parseNonNegativeLong(request, 2, "offset");
        long value = CommandSupport.parseLong(request, 3, "value");
        if (value != 0L && value != 1L) {
            return CommandSupport.error("ERR bit is not an integer or out of range");
        }
        if ((offset >>> 3) + 1L > MAX_STRING_BYTES) {
            return CommandSupport.error("ERR string exceeds maximum allowed size");
        }
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int previous = support.commandDb(execution).writes().strings()
                    .setBit(request.readOnlyByteArray(1), offset, (int) value).value();
            execution.reply().integer(previous);
        });
    }

    private PreparedCommand getbit(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        long offset = CommandSupport.parseNonNegativeLong(request, 2, "offset");
        long value = support.commandDb(context).reads().strings().getBit(support.argView(request, 1), offset);
        return CommandSupport.fixed(ReplyShapes.integer(value), execution -> execution.reply().integer(value));
    }

    private PreparedCommand bitcount(ExecutionRequest request, CommandPreparationContext context) {
        long count = request.argc() == 2
                ? support.commandDb(context).reads().strings().bitcount(support.argView(request, 1))
                : support.commandDb(context).reads().strings().bitcount(
                        support.argView(request, 1),
                        CommandSupport.parseLong(request, 2, "start"),
                        CommandSupport.parseLong(request, 3, "end")
                );
        return CommandSupport.fixed(ReplyShapes.integer(count), execution -> execution.reply().integer(count));
    }

    private PreparedCommand incrBy(ArgReader args, long delta) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            long value = support.commandDb(execution).writes().strings()
                    .incrBy(request.readOnlyByteArray(1), delta).value();
            execution.reply().integer(value);
        });
    }

    private static final class PreparedSet implements PreparedCommand {
        private final PreparedMutation<StringWriteOps.SetStringValue> mutation;
        private final StringWriteOps.SetStringValue preview;
        private final boolean getOld;
        private final ReplyShape shape;
        private boolean closed;

        private PreparedSet(
                PreparedMutation<StringWriteOps.SetStringValue> mutation,
                StringWriteOps.SetStringValue preview,
                boolean getOld,
                ReplyShape shape
        ) {
            this.mutation = Objects.requireNonNull(mutation, "mutation");
            this.preview = Objects.requireNonNull(preview, "preview");
            this.getOld = getOld;
            this.shape = Objects.requireNonNull(shape, "shape");
        }

        @Override
        public ReplyShape replyShape() {
            return shape;
        }

        @Override
        public ValidationResult validateBeforeExecute() {
            return mutation.isCurrent() ? ValidationResult.VALID : ValidationResult.STALE;
        }

        @Override
        public void execute(CommandExecutionContext context) {
            mutation.commit(context.mutationContext());
            if (!preview.applied()) {
                context.reply().nullValue();
                return;
            }
            if (!getOld) {
                context.reply().simpleString("OK");
                return;
            }
            preview.oldValue().emitTo(new BulkStringReplyAdapter(context.reply()));
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                mutation.close();
            }
        }
    }
}
