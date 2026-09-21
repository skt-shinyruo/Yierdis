package yier.bubu.redis.app.bench;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

import java.io.PrintWriter;
import java.util.function.Supplier;

public final class BenchCommands {
    private BenchCommands() {
    }

    /** 配置解析是信任边界：非法值统一转成 picocli usage error。 */
    public static <C> C parseConfig(CommandSpec spec, Supplier<C> parse) {
        try {
            return parse.get();
        } catch (IllegalArgumentException failure) {
            throw new ParameterException(spec.commandLine(), failure.getMessage(), failure);
        }
    }

    public static void writeOutput(CommandSpec spec, String rendered) {
        PrintWriter out = spec.commandLine().getOut();
        out.print(rendered);
        out.flush();
        if (out.checkError()) {
            throw new IllegalStateException("failed to write benchmark output");
        }
    }
}
