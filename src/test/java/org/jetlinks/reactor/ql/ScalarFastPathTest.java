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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ScalarFilter;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

class ScalarFastPathTest {

    @Test
    void shouldKeepProjectionResultsMutableOrderedAndIsolatedAcrossWidths() {
        assertProjectionWidth("select this as first,this + 1 as second from test", 2);
        assertProjectionWidth("select this as first,this + 1 as second,this + 2 as third,this + 3 as fourth from test", 4);
        assertProjectionWidth("select this as first,this + 1 as second,this + 2 as third,this + 3 as fourth,"
                                      + "this + 4 as fifth,this + 5 as sixth,this + 6 as seventh,this + 7 as eighth from test", 8);
    }

    @Test
    void shouldKeepCustomResultContainerForWideProjection() {
        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.just(1)) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new LinkedHashMap<>();
            }
        };

        ReactorQLRecord record = ReactorQL.builder()
                                            .sql("select this as first,this + 1 as second,this + 2 as third,this + 3 as fourth from test")
                                            .build()
                                            .start(context)
                                            .single()
                                            .block();
        Assertions.assertNotNull(record);
        Map<String, Object> result = record.asMap();

        Assertions.assertTrue(result instanceof LinkedHashMap);
        Assertions.assertEquals(1, containers.get());
        Assertions.assertEquals(List.of("first", "second", "third", "fourth"),
                                new ArrayList<>(result.keySet()));
    }

    @Test
    void shouldKeepNullAndEmptyAsyncProjectionBehavior() {
        ReactorQL.builder()
                 .sql("select null first,null second,null third,null fourth from test")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.emptyMap())
                 .verifyComplete();

        ValueMapFeature empty = asyncValue("async_empty", record -> Mono.empty());
        ReactorQL.builder()
                 .feature(empty)
                 .sql("select async_empty(this) omitted from test")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.emptyMap())
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select this as explicit,* from test")
                 .build()
                 .start(Flux.just(Collections.singletonMap("source", 1)))
                 .as(StepVerifier::create)
                 .expectNext(row("explicit", Collections.singletonMap("source", 1), "source", 1))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select t.* from test t")
                 .build()
                 .start(Flux.just(Collections.singletonMap("source", 1)))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("source", 1))
                 .verifyComplete();
    }

    private static void assertProjectionWidth(String sql, int width) {
        List<Map<String, Object>> rows = ReactorQL.builder()
                                                   .sql(sql)
                                                   .build()
                                                   .start(Flux.just(1, 2))
                                                   .collectList()
                                                   .block();
        Assertions.assertNotNull(rows);
        Assertions.assertEquals(2, rows.size());

        String[] names = {"first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth"};
        Map<String, Object> expected = new HashMap<>(4);
        for (int index = 0; index < width; index++) {
            expected.put(names[index], index == 0 ? 1 : index + 1L);
        }
        for (int index = 0; index < width; index++) {
            Object value = rows.get(0).get(names[index]);
            Assertions.assertTrue(value instanceof Number);
            Assertions.assertEquals(index + 1L, ((Number) value).longValue());
        }
        Assertions.assertEquals(new ArrayList<>(expected.keySet()), new ArrayList<>(rows.get(0).keySet()));

        rows.get(0).put("changed", true);
        Assertions.assertFalse(rows.get(1).containsKey("changed"));
    }

    @Test
    void shouldFuseScalarRowsWithoutInliningNativeCalculatorBoundaries() {
        for (boolean calculator : List.of(false, true)) {
            ReactorQL query = ReactorQL.builder()
                                      .sql(calculator
                                                   ? "select score + 1 value from test where score > 0"
                                                   : "select score value from test where score > 0")
                                      .build();
            Flux<ReactorQLRecord> result = query.start(
                    new DefaultReactorQLContext(ignore -> Flux.just(Collections.singletonMap("score", 1)))
            );
            List<String> operators = Scannable.from(result).parents().map(Scannable::stepName)
                                              .collect(java.util.stream.Collectors.toList());
            Assertions.assertEquals(1, operators.stream().filter("handle"::equals).count(), operators.toString());
            Assertions.assertFalse(operators.contains("filter"), operators.toString());
            String plan = ((DefaultReactorQL) query).describeExecutionPlan();
            Assertions.assertEquals(!calculator, plan.contains("SCALAR[where+projection,handle]"), plan);
            if (calculator) {
                Assertions.assertTrue(plan.contains("SCALAR[where]"), plan);
                Assertions.assertTrue(plan.contains("ASYNC_OR_STATEFUL[projection]"), plan);
            }
            StepVerifier.create(result.map(ReactorQLRecord::asMap))
                        .expectNext(Collections.singletonMap("value", calculator ? (Object) 2L : 1))
                        .verifyComplete();
        }
    }

    @Test
    void shouldKeepCheckpointOperatorBoundaries() {
        ReactorQL query = ReactorQL
                .builder()
                .setting("checkpoint", true)
                .sql("select score + 1 value from test where score > 0")
                .build();

        Flux<ReactorQLRecord> result = query.start(
                new DefaultReactorQLContext(ignore -> Flux.just(Collections.singletonMap("score", 1)))
        );
        List<String> operators = Scannable
                .from(result)
                .parents()
                .map(Scannable::stepName)
                .collect(java.util.stream.Collectors.toList());

        Assertions.assertFalse(operators.contains("handle"), operators.toString());
        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("DIAGNOSTIC[checkpoint]"));
    }

    @Test
    void shouldRetainFuseableSourceAndSupportHiddenSource() {
        ReactorQLRecord record = ReactorQLRecord.newRecord(
                null,
                1,
                new DefaultReactorQLContext(ignore -> Flux.empty())
        );
        SynchronousRowStage stage = new SynchronousRowStage(
                (ScalarFilter) (ctx, value) -> true,
                Function.identity()
        );

        Assertions.assertTrue(stage.apply(Flux.just(record)) instanceof Fuseable);
        Assertions.assertFalse(stage.apply(Flux.just(record).hide()) instanceof Fuseable);
    }

    @Test
    void shouldHonorBackpressureCancellationAndContext() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean contextVisible = new AtomicBoolean();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select this value from test where this >= 0")
                .build();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux
                .deferContextual(contextView -> {
                    contextVisible.set(contextView.hasKey(ReactorQLContext.class));
                    return Flux.range(0, 1000);
                })
                .doOnCancel(() -> cancelled.set(true)));

        StepVerifier
                .create(query.start(context), 0)
                .expectSubscription()
                .thenRequest(1)
                .assertNext(record -> Assertions.assertEquals(0, record.asMap().get("value")))
                .thenCancel()
                .verify();

        Assertions.assertTrue(contextVisible.get());
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldReuseCompiledStageAcrossConcurrentSubscriptions() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select this + 1 value from test where this >= 0")
                .build();

        Mono<List<Map<String, Object>>> first = query.start(Flux.range(0, 100)).collectList();
        Mono<List<Map<String, Object>>> second = query.start(Flux.range(100, 100)).collectList();

        StepVerifier
                .create(Mono.zip(first, second))
                .assertNext(result -> {
                    Assertions.assertEquals(Collections.singletonMap("value", 1L), result.getT1().get(0));
                    Assertions.assertEquals(Collections.singletonMap("value", 200L), result.getT2().get(99));
                })
                .verifyComplete();
    }

    @Test
    void shouldUseScalarExpressionsAcrossStatefulStages() {
        ReactorQL
                .builder()
                .sql("select type, count(score) total, max(score) max_score from test " +
                             "group by type having total > 1 order by max_score desc")
                .build()
                .start(Flux.just(
                        row("type", "a", "score", 1),
                        row("type", "b", "score", 2),
                        row("type", "a", "score", 3)
                ))
                .as(StepVerifier::create)
                .expectNext(row("type", "a", "total", 2L, "max_score", 3))
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select distinct on(type, score + 1) type, score from test order by score desc")
                .build()
                .start(Flux.just(
                        row("type", "a", "score", 1),
                        row("type", "a", "score", 1),
                        row("type", "a", "score", 2)
                ))
                .as(StepVerifier::create)
                .expectNext(row("type", "a", "score", 2), row("type", "a", "score", 1))
                .verifyComplete();
    }

    @Test
    void shouldBoundJoinConcurrency() {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_JOIN_CONCURRENCY, 2)
                .sql("select t1.v value from t1 join t2 on t1.v = t2.v")
                .build();

        Function<String, Publisher<?>> source = table -> {
            if ("t1".equals(table)) {
                return Flux.range(0, 20).map(value -> Collections.singletonMap("v", value));
            }
            return Flux.defer(() -> {
                int now = active.incrementAndGet();
                maximum.accumulateAndGet(now, Math::max);
                return Mono
                        .delay(Duration.ofMillis(5))
                        .map(ignore -> Collections.<String, Object>singletonMap("v", 0))
                        .doOnTerminate(active::decrementAndGet);
            });
        };

        StepVerifier
                .create(query.start(source))
                .expectNext(Collections.singletonMap("value", 0))
                .verifyComplete();

        Assertions.assertTrue(maximum.get() <= 2, "max active right sources: " + maximum.get());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> ReactorQL
                        .builder()
                        .setting(DefaultReactorQL.SETTING_JOIN_CONCURRENCY, 0)
                        .sql("select * from t1 join t2")
                        .build()
        );
    }

    @Test
    void shouldComposeSynchronousProjectionAndPredicate() {
        Map<String, Object> matched = new HashMap<>();
        matched.put("name", "alpha");
        matched.put("score", 1);

        Map<String, Object> filtered = new HashMap<>();
        filtered.put("name", "beta");
        filtered.put("score", 2);

        ReactorQL
                .builder()
                .sql("select upper(name) upper_name, cast(score + 1 as long) next_score " +
                             "from test where score between 1 and 3 and name like 'a%' and missing is null")
                .build()
                .start(Flux.just(matched, filtered))
                .as(StepVerifier::create)
                .expectNext(row("upper_name", "ALPHA", "next_score", 2L))
                .verifyComplete();
    }

    @Test
    void shouldPreserveOverriddenPropertyFeatureContract() {
        org.jetlinks.reactor.ql.supports.DefaultPropertyFeature customProperty =
                new org.jetlinks.reactor.ql.supports.DefaultPropertyFeature() {
                    @Override
                    public Optional<Object> getProperty(Object property, Object source) {
                        if ("score".equals(property)) {
                            return Optional.of(10);
                        }
                        return super.getProperty(property, source);
                    }
                };

        ReactorQL
                .builder()
                .feature(customProperty)
                .sql("select score from test where score > 5")
                .build()
                .start(Flux.just(Collections.singletonMap("score", 1)))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("score", 10))
                .verifyComplete();
    }

    @Test
    void shouldKeepDottedKeyPriorityAndCustomNestedPropertyResolver() {
        Map<String, Object> row = new HashMap<>();
        row.put("payload", Collections.singletonMap("temperature", 7));
        row.put("payload.temperature", 8);
        ReactorQL builtIn = ReactorQL.builder()
                                     .sql("select payload.temperature temperature from test")
                                     .build();
        StepVerifier.create(builtIn.start(Flux.just(row)))
                    .expectNext(Collections.singletonMap("temperature", 8))
                    .verifyComplete();

        org.jetlinks.reactor.ql.supports.DefaultPropertyFeature custom =
                new org.jetlinks.reactor.ql.supports.DefaultPropertyFeature() {
                    @Override
                    public Optional<Object> getProperty(Object property, Object source) {
                        return "payload.temperature".equals(property)
                                ? Optional.of(9)
                                : super.getProperty(property, source);
                    }
                };
        StepVerifier.create(ReactorQL.builder()
                                    .feature(custom)
                                    .sql("select payload.temperature temperature from test")
                                    .build()
                                    .start(Flux.just(row)))
                    .expectNext(Collections.singletonMap("temperature", 9))
                    .verifyComplete();
    }

    @Test
    void shouldKeepCurrentValueAndCustomPropertySemantics() {
        Map<String, Object> source = Collections.singletonMap("score", 7);
        ReactorQL builtIn = ReactorQL.builder()
                .sql("select this value from test")
                .build();
        StepVerifier.create(builtIn.start(Flux.just(1, source)))
                    .expectNext(Collections.singletonMap("value", 1),
                                Collections.singletonMap("value", source))
                    .verifyComplete();

        org.jetlinks.reactor.ql.supports.DefaultPropertyFeature custom =
                new org.jetlinks.reactor.ql.supports.DefaultPropertyFeature() {
                    @Override
                    public Optional<Object> getProperty(Object property, Object value) {
                        return "this".equals(property)
                                ? Optional.of("custom")
                                : super.getProperty(property, value);
                    }
                };
        StepVerifier.create(ReactorQL.builder()
                                    .feature(custom)
                                    .sql("select this value from test")
                                    .build()
                                    .start(Flux.just(1)))
                    .expectNext(Collections.singletonMap("value", "custom"))
                    .verifyComplete();

        ReactorQLRecord derived = new DefaultReactorQLRecord(
                null,
                (Object) null,
                new DefaultReactorQLContext(ignore -> Flux.empty())
        ).setResult("score", 7);
        ScalarValueMapper currentValue = (ScalarValueMapper) new org.jetlinks.reactor.ql.supports.map.PropertyMapFeature()
                .createMapper(new net.sf.jsqlparser.schema.Column("this"),
                              new org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata(
                                      "select this from test"));
        Assertions.assertSame(derived.asMap(), currentValue.applyScalar(derived));
    }

    @Test
    void shouldKeepMapPropertyFallbacksWithCompiledColumnAccess() {
        Map<String, Object> value = new HashMap<>();
        value.put("score", 7);
        value.put("nested", Collections.singletonMap("value", 3));
        Map<String, Object> expected = new HashMap<>();
        expected.put("score", 7);
        expected.put("nested_value", 3);
        expected.put("map_size", 2);
        expected.put("raw", value);

        ReactorQL.builder()
                .sql("select t.score score,nested.value nested_value,size map_size,"
                             + "this raw,t.missing missing from test t where t.score > 0")
                .build()
                .start(Flux.just(value))
                .as(StepVerifier::create)
                .expectNext(expected)
                .verifyComplete();
    }

    @Test
    void shouldComposeBranchingCommonAndJsonFunctions() {
        Map<String, Object> value = row("name", "alpha", "score", 2);
        value.put("json", "{\"value\":3}");
        String sql = "select if(score > 1, upper(name), 'small') conditional, " +
                "case when score > 1 then replace(name, 'a', 'A') else 'small' end case_value, " +
                "coalesce(missing, name) fallback, round(pow(score, 2), 0) power, " +
                "json_get(json, '$.value') json_value from test";

        org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata metadata =
                new org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata(sql);
        boolean[] scalarColumns = {false, false, false, false, false};
        int columnIndex = 0;
        for (net.sf.jsqlparser.statement.select.SelectItem item : metadata.getSql().getSelectItems()) {
            net.sf.jsqlparser.statement.select.SelectExpressionItem expressionItem =
                    (net.sf.jsqlparser.statement.select.SelectExpressionItem) item;
            Assertions.assertEquals(scalarColumns[columnIndex++],
                    ValueMapFeature.createMapperNow(expressionItem.getExpression(), metadata)
                            instanceof ScalarValueMapper,
                    expressionItem.getExpression().toString()
            );
        }

        ReactorQL
                .builder()
                .sql(sql)
                .build()
                .start(Flux.just(value))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals("ALPHA", result.get("conditional"));
                    Assertions.assertEquals("AlphA", result.get("case_value"));
                    Assertions.assertEquals("alpha", result.get("fallback"));
                    Assertions.assertEquals(4D, result.get("power"));
                    Assertions.assertEquals(3, ((Number) result.get("json_value")).intValue());
                })
                .verifyComplete();
    }

    @Test
    void shouldEvaluateOnlySelectedSynchronousBranch() {
        ReactorQL
                .builder()
                .sql("select if(1 = 1, 1, 1 / 0) if_value, " +
                             "case when 1 = 1 then 2 when 1 = 1 then 1 / 0 else 1 / 0 end case_value, " +
                             "coalesce('ok', 1 / 0) coalesce_value from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .expectNext(row("if_value", 1L, "case_value", 2L, "coalesce_value", "ok"))
                .verifyComplete();
    }

    @Test
    void shouldExposePublisherBoundariesAndComposedScalarContracts() {
        org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata metadata =
                new org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata(
                        "select upper(name), cast(score + 1 as long) from test " +
                                "where score >= 1 and score < 3"
                );

        boolean[] scalarColumns = {false, false};
        int columnIndex = 0;
        for (net.sf.jsqlparser.statement.select.SelectItem item : metadata.getSql().getSelectItems()) {
            net.sf.jsqlparser.statement.select.SelectExpressionItem expressionItem =
                    (net.sf.jsqlparser.statement.select.SelectExpressionItem) item;
            Assertions.assertEquals(scalarColumns[columnIndex++],
                    ValueMapFeature.createMapperNow(expressionItem.getExpression(), metadata)
                            instanceof ScalarValueMapper
            );
        }
        Assertions.assertTrue(
                FilterFeature.createPredicateNow(metadata.getSql().getWhere(), metadata)
                        instanceof ScalarFilter
        );
    }

    @Test
    void shouldPreserveEmptyValueSemantics() {
        Map<String, Object> value = Collections.singletonMap("present", 1);

        ReactorQL
                .builder()
                .sql("select missing value from test where missing is null")
                .build()
                .start(Flux.just(value))
                .as(StepVerifier::create)
                .expectNext(Collections.emptyMap())
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select count(1) total from test where missing = 1")
                .build()
                .start(Flux.just(value))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 0L))
                .verifyComplete();
    }

    @Test
    void shouldKeepAsynchronousFeaturesReactive() {
        AtomicInteger valueInvocations = new AtomicInteger();
        AtomicInteger filterInvocations = new AtomicInteger();
        ValueMapFeature asyncValue = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono
                        .delay(Duration.ofMillis(1))
                        .map(ignore -> {
                            valueInvocations.incrementAndGet();
                            return ((Map<?, ?>) record.getRecord()).get("name");
                        });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("async_value").getId();
            }
        };
        FilterFeature asyncFilter = new FilterFeature() {
            @Override
            public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression,
                                                                                       ReactorQLMetadata metadata) {
                return (record, value) -> Mono
                        .delay(Duration.ofMillis(1))
                        .map(ignore -> {
                            filterInvocations.incrementAndGet();
                            return ((Number) ((Map<?, ?>) record.getRecord()).get("score")).intValue() > 1;
                        });
            }

            @Override
            public String getId() {
                return FeatureId.Filter.of("async_match").getId();
            }
        };

        ReactorQL
                .builder()
                .feature(asyncValue, asyncFilter)
                .sql("select async_value(name) value from test where async_match(score)")
                .build()
                .start(Flux.just(row("name", "alpha", "score", 1), row("name", "beta", "score", 2)))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("value", "beta"))
                .verifyComplete();

        Assertions.assertEquals(2, filterInvocations.get());
        Assertions.assertEquals(1, valueInvocations.get());
    }

    @Test
    void shouldCombineSynchronousAndAsynchronousProjectionColumns() {
        ValueMapFeature asyncValue = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono
                        .delay(Duration.ofMillis(1))
                        .map(ignore -> ((Map<?, ?>) record.getRecord()).get("name"));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("async_value").getId();
            }
        };

        ReactorQL
                .builder()
                .feature(asyncValue)
                .sql("select name, async_value(name) async_name, score + 1 next_score from test")
                .build()
                .start(Flux.just(row("name", "alpha", "score", 1)))
                .as(StepVerifier::create)
                .expectNext(row("name", "alpha", "async_name", "alpha", "next_score", 2L))
                .verifyComplete();
    }

    @Test
    void shouldPreserveEmptySingleAsyncProjectionAndContext() {
        AtomicBoolean contextVisible = new AtomicBoolean();
        ValueMapFeature asyncValue = asyncValue("async_value", record -> Mono.deferContextual(context -> {
            contextVisible.set(context.hasKey(ReactorQLContext.class));
            Object name = ((Map<?, ?>) record.getRecord()).get("name");
            return Mono.justOrEmpty(name);
        }));

        ReactorQL.builder()
                 .feature(asyncValue)
                 .sql("select score + 1 next_score,async_value(name) async_name from test")
                 .build()
                 .start(Flux.just(row("name", "alpha", "score", 1),
                                  Collections.singletonMap("score", 2)))
                 .as(StepVerifier::create)
                 .expectNext(row("next_score", 2L, "async_name", "alpha"),
                             Collections.singletonMap("next_score", 3L))
                 .verifyComplete();
        Assertions.assertTrue(contextVisible.get());
    }

    @Test
    void shouldPropagateSingleAsyncProjectionErrorAndCancellation() {
        ValueMapFeature error = asyncValue(
                "async_error",
                record -> Mono.error(new IllegalStateException("projection boom"))
        );
        ReactorQL.builder()
                 .feature(error)
                 .sql("select async_error(score) value from test")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(StepVerifier::create)
                 .expectErrorMessage("projection boom")
                 .verify();

        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature waiting = asyncValue(
                "async_wait",
                record -> Mono.never()
                              .doOnSubscribe(ignore -> subscribed.set(true))
                              .doOnCancel(() -> cancelled.set(true))
        );
        ReactorQL.builder()
                 .feature(waiting)
                 .sql("select async_wait(score) value from test")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(flux -> StepVerifier.create(flux, 0))
                 .thenRequest(1)
                 .then(() -> Assertions.assertTrue(subscribed.get()))
                 .thenCancel()
                 .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldKeepMultipleAsyncProjectionColumns() {
        ValueMapFeature first = asyncValue("async_first", record -> Mono.just("first"));
        ValueMapFeature second = asyncValue("async_second", record -> Mono.just("second"));
        ReactorQL.builder()
                 .feature(first, second)
                 .sql("select async_first(score) first_value,async_second(score) second_value from test")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(StepVerifier::create)
                 .expectNext(row("first_value", "first", "second_value", "second"))
                 .verifyComplete();
    }

    @Test
    void shouldPreserveEmptyColumnsAndSubscribeAllAsyncProjections() {
        AtomicInteger subscribed = new AtomicInteger();
        AtomicBoolean contextVisible = new AtomicBoolean();
        ValueMapFeature empty = asyncValue("async_empty", record -> Mono.empty());
        ValueMapFeature first = asyncValue("async_first", record -> Mono.deferContextual(context -> {
            contextVisible.set(context.hasKey(ReactorQLContext.class));
            return Mono.just("first");
        }).doOnSubscribe(ignore -> subscribed.incrementAndGet()));
        ValueMapFeature second = asyncValue("async_second", record -> Mono.just("second")
                .doOnSubscribe(ignore -> subscribed.incrementAndGet()));

        ReactorQL.builder()
                 .feature(empty, first, second)
                 .sql("select async_empty(score) omitted,async_first(score) first_value,"
                              + "async_second(score) second_value from test")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(flux -> StepVerifier.create(flux, 0))
                 .thenRequest(1)
                 .expectNext(row("first_value", "first", "second_value", "second"))
                 .verifyComplete();
        Assertions.assertEquals(2, subscribed.get());
        Assertions.assertTrue(contextVisible.get());
    }

    @Test
    void shouldDelayMultiAsyncErrorUntilOtherColumnTerminates() {
        Sinks.One<String> pending = Sinks.one();
        AtomicBoolean subscribed = new AtomicBoolean();
        ValueMapFeature failed = asyncValue("async_failed", record ->
                Mono.error(new IllegalStateException("first failed")));
        ValueMapFeature waiting = asyncValue("async_waiting", record -> pending.asMono()
                .doOnSubscribe(ignore -> subscribed.set(true)));

        StepVerifier.create(ReactorQL.builder()
                                     .feature(failed, waiting)
                                     .sql("select async_failed(score) failed,"
                                                  + "async_waiting(score) waiting from test")
                                     .build()
                                     .start(Flux.just(Collections.singletonMap("score", 1))))
                    .expectSubscription()
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .then(() -> pending.tryEmitValue("done"))
                    .expectErrorMessage("first failed")
                    .verify();
    }

    @Test
    void shouldCancelAllPendingAsyncProjectionColumns() {
        AtomicInteger subscribed = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        ValueMapFeature first = asyncValue("async_first", record -> Mono.never()
                .doOnSubscribe(ignore -> subscribed.incrementAndGet())
                .doOnCancel(cancelled::incrementAndGet));
        ValueMapFeature second = asyncValue("async_second", record -> Mono.never()
                .doOnSubscribe(ignore -> subscribed.incrementAndGet())
                .doOnCancel(cancelled::incrementAndGet));

        ReactorQL.builder()
                 .feature(first, second)
                 .sql("select async_first(score) first_value,async_second(score) second_value from test")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(flux -> StepVerifier.create(flux, 0))
                 .thenRequest(1)
                 .then(() -> Assertions.assertEquals(2, subscribed.get()))
                 .thenCancel()
                 .verify();
        Assertions.assertEquals(2, cancelled.get());
    }

    @Test
    void shouldPreserveAsyncTableStarProjectionValuesOrderSubscriptionsAndContext() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean contextVisible = new AtomicBoolean();
        ValueMapFeature asyncValue = asyncValue("async_star", record -> Mono.deferContextual(context -> {
            subscriptions.incrementAndGet();
            contextVisible.set(context.hasKey(ReactorQLContext.class)
                    && "star-context".equals(context.get("marker")));
            return Mono.justOrEmpty(((Map<?, ?>) record.getRecord()).get("name"));
        }));
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("name", "alpha");
        first.put("score", 1);
        Map<String, Object> expectedFirst = new HashMap<>();
        expectedFirst.put("async_name", "alpha");
        expectedFirst.put("name", "alpha");
        expectedFirst.put("score", 1);
        Map<String, Object> expectedEmpty = new HashMap<>();
        expectedEmpty.put("score", 2);

        ReactorQL.builder()
                 .feature(asyncValue)
                 .sql("select t.*,async_star(t.name) async_name from test t")
                 .build()
                 .start(Flux.just(first, Collections.singletonMap("score", 2)))
                 .contextWrite(context -> context.put("marker", "star-context"))
                 .as(StepVerifier::create)
                .assertNext(actual -> {
                     Assertions.assertEquals(expectedFirst, actual);
                     Assertions.assertEquals(java.util.Arrays.asList("name", "async_name", "score"),
                                             new ArrayList<>(actual.keySet()));
                 })
                 .assertNext(actual -> {
                     Assertions.assertEquals(expectedEmpty, actual);
                     Assertions.assertEquals(new ArrayList<>(expectedEmpty.keySet()), new ArrayList<>(actual.keySet()));
                 })
                 .verifyComplete();
        Assertions.assertEquals(2, subscriptions.get());
        Assertions.assertTrue(contextVisible.get());
    }

    @Test
    void shouldPropagateAsyncTableStarProjectionErrorAndCancellation() {
        ValueMapFeature error = asyncValue("async_star_error",
                                           record -> Mono.error(new IllegalStateException("star projection boom")));
        ReactorQL.builder()
                 .feature(error)
                 .sql("select t.*,async_star_error(t.score) value from test t")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(StepVerifier::create)
                 .expectErrorMessage("star projection boom")
                 .verify();

        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature waiting = asyncValue("async_star_wait", record -> Mono.never()
                .doOnSubscribe(ignore -> subscribed.set(true))
                .doOnCancel(() -> cancelled.set(true)));
        ReactorQL.builder()
                 .feature(waiting)
                 .sql("select t.*,async_star_wait(t.score) value from test t")
                 .build()
                 .start(Flux.just(Collections.singletonMap("score", 1)))
                 .as(flux -> StepVerifier.create(flux, 0))
                 .thenRequest(1)
                 .then(() -> Assertions.assertTrue(subscribed.get()))
                 .thenCancel()
                 .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldPropagateErrorAndCancellationFromAsynchronousFilter() {
        FilterFeature errorFilter = asyncFilter("async_error", Mono.error(new IllegalStateException("boom")));

        ReactorQL
                .builder()
                .feature(errorFilter)
                .sql("select * from test where async_error()")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .expectErrorMessage("boom")
                .verify();

        AtomicBoolean cancelled = new AtomicBoolean();
        FilterFeature waitingFilter = asyncFilter(
                "async_wait",
                Mono.<Boolean>never().doOnCancel(() -> cancelled.set(true))
        );

        ReactorQL
                .builder()
                .feature(waitingFilter)
                .sql("select * from test where async_wait()")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .expectSubscription()
                .thenAwait(Duration.ofMillis(10))
                .thenCancel()
                .verify();

        Assertions.assertTrue(cancelled.get());
    }

    private static FilterFeature asyncFilter(String name, Mono<Boolean> result) {
        return new FilterFeature() {
            @Override
            public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression,
                                                                                       ReactorQLMetadata metadata) {
                return (record, value) -> result;
            }

            @Override
            public String getId() {
                return FeatureId.Filter.of(name).getId();
            }
        };
    }

    private static ValueMapFeature asyncValue(String name,
                                              Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }

    private static Map<String, Object> row(String key1, Object value1, String key2, Object value2) {
        Map<String, Object> row = new HashMap<>();
        row.put(key1, value1);
        row.put(key2, value2);
        return row;
    }

    private static Map<String, Object> row(String key1,
                                           Object value1,
                                           String key2,
                                           Object value2,
                                           String key3,
                                           Object value3) {
        Map<String, Object> row = row(key1, value1, key2, value2);
        row.put(key3, value3);
        return row;
    }
}
