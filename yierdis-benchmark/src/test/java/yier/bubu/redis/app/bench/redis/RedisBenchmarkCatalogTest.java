package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisBenchmarkCatalogTest {
    @Test
    public void catalogMatchesOfficialBuiltInOrderAndSupport() {
        List<RedisBenchmarkCase> cases = new RedisBenchmarkCatalog().allCases();
        Assert.assertEquals(List.of(
                "PING_INLINE", "PING_MBULK", "SET", "GET", "INCR", "LPUSH", "RPUSH",
                "LPOP", "RPOP", "SADD", "HSET", "SPOP", "ZADD", "ZPOPMIN",
                "LPUSH (needed to benchmark LRANGE)",
                "LRANGE_100 (first 100 elements)",
                "LRANGE_300 (first 300 elements)",
                "LRANGE_500 (first 500 elements)",
                "LRANGE_600 (first 600 elements)",
                "MSET (10 keys)", "XADD"
        ), cases.stream().map(RedisBenchmarkCase::title).toList());

        Assert.assertEquals(List.of("spop", "zpopmin", "mset", "xadd"), cases.stream()
                .filter(testCase -> !testCase.support().supported())
                .map(RedisBenchmarkCase::id)
                .toList());
        Assert.assertEquals(17, cases.stream().filter(testCase -> testCase.support().supported()).count());
    }

    @Test
    public void selectionAliasesMatchOfficialBehavior() {
        RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
        Assert.assertEquals(List.of("PING_INLINE", "PING_MBULK"), catalog.select(Set.of("ping")).stream()
                .map(RedisBenchmarkCase::title).toList());
        Assert.assertEquals(List.of(
                "LPUSH (needed to benchmark LRANGE)",
                "LRANGE_300 (first 300 elements)"
        ), catalog.select(Set.of("lrange_300")).stream().map(RedisBenchmarkCase::title).toList());
        Assert.assertEquals(List.of(
                "LPUSH (needed to benchmark LRANGE)",
                "LRANGE_100 (first 100 elements)",
                "LRANGE_300 (first 300 elements)",
                "LRANGE_500 (first 500 elements)",
                "LRANGE_600 (first 600 elements)"
        ), catalog.select(Set.of("lrange")).stream().map(RedisBenchmarkCase::title).toList());
        Assert.assertThrows(IllegalArgumentException.class, () -> catalog.select(Set.of("no_such_test")));
    }

    @Test
    public void emptySelectorsReturnAllCasesInCanonicalOrder() {
        RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();

        Assert.assertEquals(21, catalog.select(Set.of()).size());
        Assert.assertEquals(
                catalog.allCases().stream().map(RedisBenchmarkCase::id).toList(),
                catalog.select(Set.of()).stream().map(RedisBenchmarkCase::id).toList()
        );
    }

    @Test
    public void multipleSelectorsProduceDeduplicatedCanonicalOrderUnion() {
        RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();

        Assert.assertEquals(List.of(
                "ping_inline", "ping_mbulk", "get", "lrange_setup", "lrange_300"
        ), catalog.select(Set.of(" PING ", "ping_inline", " GET ", "LRANGE_300")).stream()
                .map(RedisBenchmarkCase::id)
                .toList());
        Assert.assertThrows(IllegalArgumentException.class,
                () -> catalog.select(Set.of("set", "no_such_test")));
    }

    @Test
    public void caseByIdNormalizesInputAndRejectsUnknownIds() {
        RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();

        Assert.assertSame(catalog.allCases().get(2), catalog.caseById(" SET "));
        Assert.assertThrows(IllegalArgumentException.class, () -> catalog.caseById(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> catalog.caseById(" "));
        Assert.assertThrows(IllegalArgumentException.class, () -> catalog.caseById("no_such_case"));
    }

    @Test
    public void exactCaseMetadataIsDeclared() {
        List<RedisBenchmarkCase> cases = new RedisBenchmarkCatalog().allCases();

        Assert.assertEquals(List.of(
                "ping_inline", "ping_mbulk", "set", "get", "incr", "lpush", "rpush",
                "lpop", "rpop", "sadd", "hset", "spop", "zadd", "zpopmin",
                "lrange_setup", "lrange_100", "lrange_300", "lrange_500", "lrange_600",
                "mset", "xadd"
        ), cases.stream().map(RedisBenchmarkCase::id).toList());
        Assert.assertEquals(List.of(
                Set.of("ping_inline", "ping"), Set.of("ping_mbulk", "ping"),
                Set.of("set"), Set.of("get"), Set.of("incr"), Set.of("lpush"),
                Set.of("rpush"), Set.of("lpop"), Set.of("rpop"), Set.of("sadd"),
                Set.of("hset"), Set.of("spop"), Set.of("zadd"), Set.of("zpopmin"),
                Set.of("lrange", "lrange_100", "lrange_300", "lrange_500", "lrange_600"),
                Set.of("lrange", "lrange_100"), Set.of("lrange", "lrange_300"),
                Set.of("lrange", "lrange_500"), Set.of("lrange", "lrange_600"),
                Set.of("mset"), Set.of("xadd")
        ), cases.stream().map(RedisBenchmarkCase::selectionTriggers).toList());
        Assert.assertEquals(List.of(
                Set.of("PING"), Set.of("PING"), Set.of("SET"), Set.of("GET"), Set.of("INCR"),
                Set.of("LPUSH"), Set.of("RPUSH"), Set.of("LPOP"), Set.of("RPOP"), Set.of("SADD"),
                Set.of("HSET"), Set.of("SPOP"), Set.of("ZADD"), Set.of("ZPOPMIN"), Set.of("LPUSH"),
                Set.of("LRANGE"), Set.of("LRANGE"), Set.of("LRANGE"), Set.of("LRANGE"),
                Set.of("MSET"), Set.of("XADD")
        ), cases.stream().map(RedisBenchmarkCase::requiredCommands).toList());
        Assert.assertEquals(List.of(
                BenchmarkReplyExpectation.PONG, BenchmarkReplyExpectation.PONG,
                BenchmarkReplyExpectation.OK, BenchmarkReplyExpectation.BULK_OR_NULL,
                BenchmarkReplyExpectation.INTEGER, BenchmarkReplyExpectation.INTEGER,
                BenchmarkReplyExpectation.INTEGER, BenchmarkReplyExpectation.BULK_OR_NULL,
                BenchmarkReplyExpectation.BULK_OR_NULL, BenchmarkReplyExpectation.INTEGER,
                BenchmarkReplyExpectation.INTEGER, BenchmarkReplyExpectation.BULK_OR_NULL,
                BenchmarkReplyExpectation.INTEGER, BenchmarkReplyExpectation.ARRAY,
                BenchmarkReplyExpectation.INTEGER, BenchmarkReplyExpectation.ARRAY,
                BenchmarkReplyExpectation.ARRAY, BenchmarkReplyExpectation.ARRAY,
                BenchmarkReplyExpectation.ARRAY, BenchmarkReplyExpectation.OK,
                BenchmarkReplyExpectation.BULK_OR_NULL
        ), cases.stream().map(RedisBenchmarkCase::replyExpectation).toList());
        Assert.assertEquals(List.of(
                "", "", "", "", "", "", "", "", "", "", "",
                "Yierdis does not support SPOP", "", "Yierdis does not support ZPOPMIN",
                "", "", "", "", "", "Yierdis does not support MSET",
                "Yierdis does not support XADD"
        ), cases.stream().map(testCase -> testCase.support().reason()).toList());
        Assert.assertEquals(List.of(
                "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                "lrange_setup", "lrange_setup", "lrange_setup", "lrange_setup", "", ""
        ), cases.stream().map(RedisBenchmarkCase::dependencyId).toList());
    }

    @Test
    public void templatesMatchOfficialWireDeclarations() {
        List<String> templates = new RedisBenchmarkCatalog().allCases().stream()
                .map(RedisBenchmarkCase::template)
                .map(RedisBenchmarkCatalogTest::describeTemplate)
                .toList();

        Assert.assertEquals(List.of(
                "INLINE:PING\r\n",
                "RESP:PING",
                "RESP:SET|key:__rand_int__|<PAYLOAD>",
                "RESP:GET|key:__rand_int__",
                "RESP:INCR|counter:__rand_int__",
                "RESP:LPUSH|mylist|<PAYLOAD>",
                "RESP:RPUSH|mylist|<PAYLOAD>",
                "RESP:LPOP|mylist",
                "RESP:RPOP|mylist",
                "RESP:SADD|myset|element:__rand_int__",
                "RESP:HSET|myhash|element:__rand_int__|<PAYLOAD>",
                "RESP:SPOP|myset",
                "RESP:ZADD|myzset|<RANDOM_SCORE>|element:__rand_int__",
                "RESP:ZPOPMIN|myzset",
                "RESP:LPUSH|mylist|<PAYLOAD>",
                "RESP:LRANGE|mylist|0|99",
                "RESP:LRANGE|mylist|0|299",
                "RESP:LRANGE|mylist|0|499",
                "RESP:LRANGE|mylist|0|599",
                "RESP:MSET|key:__rand_int__|<PAYLOAD>|key:__rand_int__|<PAYLOAD>"
                        + "|key:__rand_int__|<PAYLOAD>|key:__rand_int__|<PAYLOAD>"
                        + "|key:__rand_int__|<PAYLOAD>|key:__rand_int__|<PAYLOAD>"
                        + "|key:__rand_int__|<PAYLOAD>|key:__rand_int__|<PAYLOAD>"
                        + "|key:__rand_int__|<PAYLOAD>|key:__rand_int__|<PAYLOAD>",
                "RESP:XADD|mystream|*|myfield|<PAYLOAD>"
        ), templates);
    }

    @Test
    public void declarationCollectionsAndByteArraysAreDefensivelyImmutable() {
        RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
        List<RedisBenchmarkCase> cases = catalog.allCases();
        Assert.assertThrows(UnsupportedOperationException.class, () -> cases.remove(0));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> catalog.select(Set.of("ping")).clear());

        RedisBenchmarkCase setCase = catalog.caseById("set");
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> setCase.selectionTriggers().add("other"));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> setCase.requiredCommands().add("OTHER"));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> setCase.template().arguments().clear());

        RedisBenchmarkCommandTemplate inline = catalog.caseById("ping_inline").template();
        byte[] inlineFrame = inline.inlineFrame();
        inlineFrame[0] = 'X';
        Assert.assertArrayEquals("PING\r\n".getBytes(StandardCharsets.US_ASCII), inline.inlineFrame());

        RedisBenchmarkCommandTemplate.Argument command = setCase.template().arguments().get(0);
        byte[] literal = command.literal();
        literal[0] = 'X';
        Assert.assertArrayEquals("SET".getBytes(StandardCharsets.US_ASCII), command.literal());

        byte[] sourceLiteral = "PING".getBytes(StandardCharsets.US_ASCII);
        RedisBenchmarkCommandTemplate.Argument sourceArgument = new RedisBenchmarkCommandTemplate.Argument(
                RedisBenchmarkCommandTemplate.ArgumentKind.LITERAL, sourceLiteral
        );
        sourceLiteral[0] = 'X';
        Assert.assertArrayEquals("PING".getBytes(StandardCharsets.US_ASCII), sourceArgument.literal());

        RedisBenchmarkCommandTemplate.Argument[] sourceArguments = {sourceArgument};
        RedisBenchmarkCommandTemplate template = RedisBenchmarkCommandTemplate.resp(sourceArguments);
        sourceArguments[0] = RedisBenchmarkCommandTemplate.Argument.literal("GET");
        Assert.assertEquals("PING", new String(
                template.arguments().get(0).literal(), StandardCharsets.US_ASCII
        ));

        Set<String> sourceTriggers = new HashSet<>(Set.of(" CUSTOM "));
        Set<String> sourceCommands = new HashSet<>(Set.of(" ping "));
        RedisBenchmarkCase custom = new RedisBenchmarkCase(
                " CUSTOM ", " Custom case ", sourceTriggers, template, sourceCommands,
                BenchmarkReplyExpectation.PONG, RedisBenchmarkCase.Support.available(), null
        );
        sourceTriggers.clear();
        sourceCommands.clear();
        Assert.assertEquals(Set.of("custom"), custom.selectionTriggers());
        Assert.assertEquals(Set.of("PING"), custom.requiredCommands());
        Assert.assertEquals("", custom.dependencyId());
    }

    @Test
    public void invalidDeclarationsAreRejectedAtTheModelBoundary() {
        RedisBenchmarkCommandTemplate template = RedisBenchmarkCommandTemplate.resp(
                RedisBenchmarkCommandTemplate.Argument.literal("PING")
        );

        Assert.assertThrows(IllegalArgumentException.class, () -> benchmarkCase(" ", "title", template));
        Assert.assertThrows(IllegalArgumentException.class, () -> benchmarkCase("id", " ", template));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RedisBenchmarkCase(
                "id", "title", Set.of(), template, Set.of("PING"), BenchmarkReplyExpectation.PONG,
                RedisBenchmarkCase.Support.available(), ""
        ));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RedisBenchmarkCase(
                "id", "title", Set.of("id"), template, Set.of(), BenchmarkReplyExpectation.PONG,
                RedisBenchmarkCase.Support.available(), ""
        ));
        Assert.assertThrows(NullPointerException.class, () -> new RedisBenchmarkCase(
                "id", "title", null, template, Set.of("PING"), BenchmarkReplyExpectation.PONG,
                RedisBenchmarkCase.Support.available(), ""
        ));
        Assert.assertThrows(NullPointerException.class, () -> new RedisBenchmarkCase(
                "id", "title", Set.of("id"), null, Set.of("PING"), BenchmarkReplyExpectation.PONG,
                RedisBenchmarkCase.Support.available(), ""
        ));
        Assert.assertThrows(NullPointerException.class, () -> new RedisBenchmarkCase(
                "id", "title", Set.of("id"), template, Set.of("PING"), null,
                RedisBenchmarkCase.Support.available(), ""
        ));
        Assert.assertThrows(NullPointerException.class, () -> new RedisBenchmarkCase(
                "id", "title", Set.of("id"), template, Set.of("PING"), BenchmarkReplyExpectation.PONG,
                null, ""
        ));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> new RedisBenchmarkCase.Support(true, "unexpected reason"));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisBenchmarkCase.Support.unsupported(" "));
        Assert.assertThrows(NullPointerException.class,
                () -> new RedisBenchmarkCase.Support(true, null));
        Assert.assertThrows(NullPointerException.class,
                () -> new RedisBenchmarkCommandTemplate.Argument(null, new byte[0]));
        Assert.assertThrows(NullPointerException.class,
                () -> new RedisBenchmarkCommandTemplate.Argument(
                        RedisBenchmarkCommandTemplate.ArgumentKind.LITERAL, null
                ));
        Assert.assertThrows(NullPointerException.class,
                () -> RedisBenchmarkCommandTemplate.Argument.literal(null));
        Assert.assertThrows(NullPointerException.class, () -> RedisBenchmarkCommandTemplate.inline(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> RedisBenchmarkCommandTemplate.inline("PING"));
        Assert.assertThrows(IllegalArgumentException.class, () -> RedisBenchmarkCommandTemplate.inline("\r\n"));
        Assert.assertThrows(IllegalArgumentException.class, RedisBenchmarkCommandTemplate::resp);
        Assert.assertThrows(NullPointerException.class,
                () -> RedisBenchmarkCommandTemplate.resp((RedisBenchmarkCommandTemplate.Argument[]) null));
        Assert.assertThrows(NullPointerException.class,
                () -> RedisBenchmarkCommandTemplate.resp((RedisBenchmarkCommandTemplate.Argument) null));
    }

    private static RedisBenchmarkCase benchmarkCase(
            String id,
            String title,
            RedisBenchmarkCommandTemplate template
    ) {
        return new RedisBenchmarkCase(
                id, title, Set.of("test"), template, Set.of("PING"),
                BenchmarkReplyExpectation.PONG, RedisBenchmarkCase.Support.available(), ""
        );
    }

    private static String describeTemplate(RedisBenchmarkCommandTemplate template) {
        if (template.wireMode() == RedisBenchmarkCommandTemplate.WireMode.INLINE) {
            Assert.assertTrue(template.arguments().isEmpty());
            return "INLINE:" + new String(template.inlineFrame(), StandardCharsets.US_ASCII);
        }
        Assert.assertArrayEquals(new byte[0], template.inlineFrame());
        return "RESP:" + template.arguments().stream()
                .map(RedisBenchmarkCatalogTest::describeArgument)
                .collect(Collectors.joining("|"));
    }

    private static String describeArgument(RedisBenchmarkCommandTemplate.Argument argument) {
        return switch (argument.kind()) {
            case LITERAL -> new String(argument.literal(), StandardCharsets.US_ASCII);
            case PAYLOAD -> {
                Assert.assertArrayEquals(new byte[0], argument.literal());
                yield "<PAYLOAD>";
            }
            case RANDOM_SCORE -> {
                Assert.assertArrayEquals(new byte[0], argument.literal());
                yield "<RANDOM_SCORE>";
            }
        };
    }
}
