package yier.bubu.redis.command.api;

import java.util.Objects;
import yier.bubu.redis.execution.api.ExecutionRequest;

/**
 * 命令语法、解析和无副作用准备三个阶段的最终注册定义。
 */
public record CommandDefinition<T>(
        CommandSyntax syntax,
        CommandParser<T> parser,
        CommandPreparer<T> preparer
) {
    public CommandDefinition {
        Objects.requireNonNull(syntax, "syntax");
        Objects.requireNonNull(parser, "parser");
        Objects.requireNonNull(preparer, "preparer");
    }

    public CommandParseResult<T> parse(ExecutionRequest request) {
        ArgReader args = ArgReader.of(Objects.requireNonNull(request, "request"));
        CommandParseError arityError = syntax.arity().validate(syntax.nameLower(), args);
        return arityError == null
                ? parser.parse(args)
                : CommandParseResult.error(arityError);
    }
}
