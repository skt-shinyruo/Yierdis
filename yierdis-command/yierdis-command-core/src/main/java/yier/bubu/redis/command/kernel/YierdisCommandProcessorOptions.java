package yier.bubu.redis.command.kernel;

/**
 * Command processor composition options.
 */
public final class YierdisCommandProcessorOptions {
    public static final YierdisCommandProcessorOptions DEFAULT = builder().build();

    private final CommandChangeObserver changeObserver;

    private YierdisCommandProcessorOptions(Builder builder) {
        this.changeObserver = builder.changeObserver == null ? CommandChangeObserver.NOOP : builder.changeObserver;
    }

    public static Builder builder() {
        return new Builder();
    }

    public CommandChangeObserver changeObserver() {
        return changeObserver;
    }

    public static final class Builder {
        private CommandChangeObserver changeObserver = CommandChangeObserver.NOOP;

        private Builder() {
        }

        public Builder changeObserver(CommandChangeObserver changeObserver) {
            this.changeObserver = changeObserver == null ? CommandChangeObserver.NOOP : changeObserver;
            return this;
        }

        public YierdisCommandProcessorOptions build() {
            return new YierdisCommandProcessorOptions(this);
        }
    }
}
