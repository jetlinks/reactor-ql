/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DialectQueryTest {

    @ParameterizedTest
    @ValueSource(strings = {"IS NOT DISTINCT FROM", "<=>"})
    void nullSafeEqualityHandlesMissingAndCoercedValues(String operator) {
        ReactorQL query = query("select id from test where a " + operator + " b order by id");
        StepVerifier.create(query.start(Flux.just(row("id", 1), row("id", 2, "a", 1),
                        row("id", 3, "a", 1, "b", 1.0), row("id", 4, "a", "2", "b", 2))))
                .expectNext(row("id", 1), row("id", 3), row("id", 4)).verifyComplete();
    }

    @Test
    void distinctFromAndBooleanTestsWorkInProjectionAndConditions() {
        ReactorQL query = query("select a is distinct from b different, a is null missing, "
                + "a is unknown unknownVal, a is not true notTrue, a is false isFalse from test");
        StepVerifier.create(query.start(Flux.just(row(), row("a", false), row("a", true, "b", true))))
                .expectNext(row("different", false, "missing", true, "unknownVal", true, "notTrue", false, "isFalse", false))
                .expectNext(row("different", true, "missing", false, "unknownVal", false, "notTrue", true, "isFalse", true))
                .expectNext(row("different", false, "missing", false, "unknownVal", false, "notTrue", false, "isFalse", false))
                .verifyComplete();
        StepVerifier.create(query("select this v from test where true and not false").start(Flux.just(1, 2)))
                .expectNext(row("v", 1), row("v", 2)).verifyComplete();
    }

    @Test
    void nullSafeAsyncOperandsRetainEmptyAndErrorSemantics() {
        RuntimeException failure = new IllegalStateException("async operand");
        ValueMapFeature value = new ValueMapFeature() {
            @Override
            public String getId() {
                return FeatureId.ValueMap.of("async_value").getId();
            }

            @Override
            public java.util.function.Function<ReactorQLRecord, org.reactivestreams.Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression, ReactorQLMetadata metadata) {
                return record -> Mono.deferContextual(context -> {
                    if (context.hasKey("fail")) {
                        return Mono.error(failure);
                    }
                    if (context.hasKey("empty")) {
                        return Mono.empty();
                    }
                    return Mono.justOrEmpty(record.getRecordValue("this"));
                });
            }
        };
        ReactorQL query = ReactorQL.builder().feature(value)
                .sql("select async_value() is not distinct from null same from test").build();
        StepVerifier.create(query.start(Flux.just(1))).expectNext(row("same", false)).verifyComplete();
        StepVerifier.create(query.start(Flux.just(1)).contextWrite(context -> context.put("fail", true)))
                .expectErrorMatches(error -> error == failure).verify();
        StepVerifier.create(query.start(Flux.just(1)).contextWrite(context -> context.put("empty", true)))
                .expectNext(row("same", true)).verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"select this v from test limit 2 offset 1",
            "select this v from test limit 1,2",
            "select this v from test offset 1 rows fetch next 2 rows only"})
    void paginationDialectsAgreeAndCancelUpstream(String sql) {
        AtomicBoolean cancelled = new AtomicBoolean();
        StepVerifier.create(query(sql).start(Flux.range(0, 10).doOnCancel(() -> cancelled.set(true))))
                .expectNext(row("v", 1), row("v", 2)).verifyComplete();
        assertTrue(cancelled.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"select top (2) this v from test", "select this v from test fetch first 2 rows only"})
    void topAndFetchReturnTheRequestedCount(String sql) {
        StepVerifier.create(query(sql).start(Flux.range(0, 4)))
                .expectNext(row("v", 0), row("v", 1)).verifyComplete();
    }

    @Test
    void namedPagingParametersAreResolvedForEachSubscription() {
        ReactorQL query = query("select this v from test offset :start rows fetch next :count rows only");
        ReactorQLContext context = ReactorQLContext.ofDatasource(ignored -> Flux.range(0, 5));
        Flux<Map<String, Object>> result = query.start(context).map(ReactorQLRecord::asMap);
        for (int start = 0; start < 3; start++) {
            context.bind("start", start).bind("count", 1);
            StepVerifier.create(result)
                    .expectNext(row("v", start)).verifyComplete();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"select top 10 percent * from test", "select top 1 with ties * from test",
            "select * from test order by this fetch first 1 rows with ties"})
    void unsupportedPagingModifiersFailAtConstruction(String sql) {
        assertThrows(ReactorQLException.class, () -> query(sql));
    }

    @Test
    void prewhereFiltersBeforeJoinAndAggregation() {
        AtomicInteger joinedSubscriptions = new AtomicInteger();
        ReactorQL query = query("select count(1) total from events e join lookup l on e.id = l.id "
                + "prewhere e.id >= 2 where l.id < 4");
        StepVerifier.create(query.start(name -> {
            if ("events".equals(name)) {
                return Flux.range(0, 5).map(id -> row("id", id));
            }
            return Flux.defer(() -> {
                joinedSubscriptions.incrementAndGet();
                return Flux.range(0, 5).map(id -> row("id", id));
            });
        })).expectNext(row("total", 2L)).verifyComplete();
        assertEquals(3, joinedSubscriptions.get());
    }

    @Test
    void prewhereCannotBeBypassedByTheRawWhereOptimization() {
        ReactorQL query = ReactorQL.builder().setting(DefaultReactorQL.SETTING_ROW_INFO_ENABLED, false)
                .sql("select id from events prewhere id > 1 where id < 3").build();
        StepVerifier.create(query.start(Flux.range(0, 5).map(id -> row("id", id))))
                .expectNext(row("id", 2)).verifyComplete();
        assertTrue(((DefaultReactorQL) query).describeExecutionPlan().contains("prewhere"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"schema.plain", "\"user@example.com\"", "\"a.b@c\""})
    void rawFilterUsesTheSameCompleteSourceNameAsTheRecordPath(String source) {
        ReactorQL query = ReactorQL.builder().setting(DefaultReactorQL.SETTING_ROW_INFO_ENABLED, false)
                .sql("select id from " + source + " where id > 1").build();
        String expected = source.replace("\"", "");
        StepVerifier.create(query.start(name -> {
            assertEquals(expected, name);
            return Flux.just(row("id", 1), row("id", 2));
        })).expectNext(row("id", 2)).verifyComplete();
    }

    @Test
    void distinctrowUsesTheExistingDistinctState() {
        StepVerifier.create(query("select distinctrow this v from test").start(Flux.just(1, 1, 2, 2)))
                .expectNext(row("v", 1), row("v", 2)).verifyComplete();
    }

    @Test
    void ilikeSupportsUnicodeSingleCharacterWildcardsAndLiteralRegexCharacters() {
        ReactorQL query = query("select this v from test where this ilike 'ä_.*%'");
        StepVerifier.create(query.start(Flux.just("Äx.*abc", "äz.*", "ÄxZZabc", "ax.*abc")))
                .expectNext(row("v", "Äx.*abc"), row("v", "äz.*")).verifyComplete();
        StepVerifier.create(query("select this v from test where this not ilike 'a%'").start(Flux.just("Ab", "bc")))
                .expectNext(row("v", "bc")).verifyComplete();
    }

    @Test
    void explicitLikeEscapeAndDynamicPatternsAreEvaluated() {
        StepVerifier.create(query("select this v from test where this like 'a!_%' escape '!'")
                        .start(Flux.just("a_x", "abx", "a_")))
                .expectNext(row("v", "a_x"), row("v", "a_")).verifyComplete();
        ReactorQL dynamic = query("select id from test where text ilike pattern escape esc");
        StepVerifier.create(dynamic.start(Flux.just(row("id", 1, "text", "Ab_C", "pattern", "ab!_c", "esc", "!"),
                        row("id", 2, "text", "AbxC", "pattern", "ab!_c", "esc", "!"))))
                .expectNext(row("id", 1)).verifyComplete();
    }

    @Test
    void standardTrimAndJsonReturningShareFunctionSemantics() {
        StepVerifier.create(query("select trim(leading 'x' from text) leadingVal, "
                + "trim(trailing 'x' from text) trailingVal, trim(both 'x' from text) bothVal, "
                + "json_value(payload, '$.id' returning bigint) id from test")
                .start(Flux.just(row("text", "xxabxx", "payload", "{\"id\":42}"))))
                .expectNext(row("leadingVal", "abxx", "trailingVal", "xxab", "bothVal", "ab", "id", 42L))
                .verifyComplete();
    }

    @Test
    void nativeJsonAndLegacyOverloadsCanBeNestedWithoutChangingLiteralsOrComments() {
        ReactorQL query = query("select json_value(payload, '$.missing', concat('a,b', ')')) fallbackVal, "
                + "json_exists(payload /* root */) rootVal, json_query(payload, '$.a') a, "
                + "'json_exists(x)' literalVal from test");
        StepVerifier.create(query.start(Flux.just(row("payload", "{\"a\":[1,2]}"))))
                .expectNext(row("fallbackVal", "a,b)", "rootVal", true, "a", Arrays.asList(1, 2),
                        "literalVal", "json_exists(x)"))
                .verifyComplete();
    }

    @Test
    void nativeJsonOptionsAreNeverSilentlyIgnored() {
        assertThrows(ReactorQLException.class, () -> query(
                "select json_value(payload, '$.id' default 99 on empty) id from test"));
    }

    @Test
    void standardExtractUsesTheDateFunctionSpi() {
        StepVerifier.create(query("select extract(year from ts) yearVal, extract(hour from ts) hourVal from test")
                        .start(Flux.just(row("ts", "2026-10-21 13:45:00"))))
                .expectNext(row("yearVal", 2026, "hourVal", 13)).verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"select value collate utf8_bin from test", "select value at time zone 'UTC' from test",
            "select sum(value) over () from test"})
    void unsupportedNativeParentsCannotResolveToTheirChildExpression(String sql) {
        assertThrows(ReactorQLException.class, () -> query(sql));
    }

    @Test
    void groupedJoinsCannotSilentlyDropTheirRightSource() {
        assertThrows(ReactorQLException.class, () -> query(
                "select * from (events e join lookup l on e.id = l.id)"));
    }

    @Test
    void jsonOperatorChainsSupportStaticAndDynamicKeysAndArrayIndexes() {
        ReactorQL query = query("select payload->'items'->0->>'id' fixedVal, "
                + "payload->field->idx->>'id' dynamicVal from test");
        StepVerifier.create(query.start(Flux.just(row("payload", "{\"items\":[{\"id\":12},{\"id\":34}]}",
                        "field", "items", "idx", 1))))
                .expectNext(row("fixedVal", "12", "dynamicVal", "34")).verifyComplete();
    }

    @Test
    void correlatedPrewhereIsNotSharedAcrossOuterRows() {
        ReactorQL query = ReactorQL.builder().setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, true)
                .sql("select (select count(1) from lookup l prewhere l.id = o.id) hits from events o").build();
        StepVerifier.create(query.start(name -> "events".equals(name)
                        ? Flux.just(row("id", 1), row("id", 2))
                        : Flux.just(row("id", 1), row("id", 1), row("id", 2))))
                .expectNext(row("hits", row("count(1)", 2L)), row("hits", row("count(1)", 1L))).verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"extract", "json_value", "trim"})
    void nativeFunctionExtensionsAreNotCachedWithoutAnExplicitPurityContract(String name) {
        String expression = "extract".equals(name) ? "extract(year from ts)"
                : "json_value".equals(name) ? "json_value(payload, '$.id')" : "trim(text)";
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder().setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, true)
                .feature(org.jetlinks.reactor.ql.supports.map.FunctionMapFeature.scalar(name, 2, 1,
                        values -> calls.incrementAndGet()))
                .sql("select (select " + expression + " from lookup) v from events").build();
        StepVerifier.create(query.start(source -> "events".equals(source) ? Flux.range(0, 3)
                        : Flux.defer(() -> {
                            subscriptions.incrementAndGet();
                            return Flux.just(row("ts", "2026-10-21", "payload", "{\"id\":1}", "text", " x "));
                        })))
                .expectNextCount(3).verifyComplete();
        assertEquals(3, calls.get());
        assertEquals(3, subscriptions.get());
    }

    private static ReactorQL query(String sql) {
        return ReactorQL.builder().sql(sql).build();
    }

    private static Map<String, Object> row(Object... entries) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            row.put((String) entries[index], entries[index + 1]);
        }
        return row;
    }
}
