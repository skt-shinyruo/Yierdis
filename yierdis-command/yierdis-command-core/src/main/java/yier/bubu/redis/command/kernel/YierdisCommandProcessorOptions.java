package yier.bubu.redis.command.kernel;

import yier.bubu.redis.runtime.api.YierdisChangeSink;

/**
 * Command processor composition options.
 */
public final class YierdisCommandProcessorOptions {
    public static final YierdisCommandProcessorOptions DEFAULT = builder().build();

    private final YierdisChangeSink changeSink;

    private YierdisCommandProcessorOptions(Builder builder) {
        this.changeSink = builder.changeSink == null ? YierdisChangeSink.NOOP : builder.changeSink;
    }

    public static Builder builder() {
        return new Builder();
    }

    public YierdisChangeSink changeSink() {
        return changeSink;
    }

    public static final class Builder {
        private YierdisChangeSink changeSink = YierdisChangeSink.NOOP;

        private Builder() {
        }

        public Builder changeSink(YierdisChangeSink changeSink) {
            this.changeSink = changeSink == null ? YierdisChangeSink.NOOP : changeSink;
            return this;
        }

        public YierdisCommandProcessorOptions build() {
            return new YierdisCommandProcessorOptions(this);
        }
    }
}
