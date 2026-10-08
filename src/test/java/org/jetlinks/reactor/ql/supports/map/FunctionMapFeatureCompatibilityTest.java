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
package org.jetlinks.reactor.ql.supports.map;

import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.DefaultReactorQLContext;
import org.jetlinks.reactor.ql.DefaultReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.Exceptions;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class FunctionMapFeatureCompatibilityTest {

    @Test
    void testOrdinaryResultAdapterKeepsAssemblyTimeScalarCallableEvaluation() {
        CallableValuePublisher source = new CallableValuePublisher(7);
        FunctionMapFeature feature = new FunctionMapFeature("callable_result", 1, 1, values -> source);
        Function<ReactorQLRecord, Publisher<?>> retained = valueMapper(
                "select callable_result(this) value from test", feature);
        ReactorQLRecord record = valueRecord("input");

        Publisher<?> original = retained.apply(record);
        Assertions.assertEquals(1, source.calls.get());
        Assertions.assertEquals(0, source.subscriptions.get());
        StepVerifier.create(Mono.<Object>from(original), 0)
                    .thenRequest(1)
                    .expectNext(7)
                    .verifyComplete();
        Assertions.assertEquals(1, source.calls.get());

        // A direct protected-apply result is the proposed adapter removal, not another oracle.
        Publisher<Object> direct = feature.apply(record, Collections.singletonList(row -> Mono.just(1)));
        Assertions.assertSame(source, direct);
        Assertions.assertEquals(1, source.calls.get());
        Assertions.assertEquals(0, source.subscriptions.get());
        StepVerifier.create(Mono.from(direct), 0)
                    .then(() -> {
                        // Reactor 3.4 MonoCallable computes at subscribe, not at the first request.
                        Assertions.assertEquals(2, source.calls.get());
                        Assertions.assertEquals(1, source.subscriptions.get());
                    })
                    .thenRequest(1)
                    .expectNext(7)
                    .verifyComplete();
        Assertions.assertEquals(2, source.calls.get());
    }

    @Test
    void testOrdinaryResultAdapterKeepsAssemblyTimeEmptyScalarCallableEvaluation() {
        CallableValuePublisher source = new CallableValuePublisher(null);
        FunctionMapFeature feature = new FunctionMapFeature("empty_callable_result", 1, 1, values -> source);
        Function<ReactorQLRecord, Publisher<?>> retained = valueMapper(
                "select empty_callable_result(this) value from test", feature);
        ReactorQLRecord record = valueRecord("input");
        StepVerifier.create(Mono.from(retained.apply(record)), 0).verifyComplete();
        Assertions.assertEquals(1, source.calls.get());
        Assertions.assertEquals(0, source.subscriptions.get());

        Publisher<Object> direct = feature.apply(record, Collections.singletonList(row -> Mono.just(1)));
        Assertions.assertEquals(1, source.calls.get());
        Assertions.assertEquals(0, source.subscriptions.get());
        StepVerifier.create(Mono.from(direct), 0).verifyComplete();
        Assertions.assertEquals(2, source.calls.get());
        Assertions.assertEquals(1, source.subscriptions.get());
    }

    @Test
    void testValueMappingKeepsColdEvaluationDefaultsAndNullErrors() {
        AtomicInteger calls = new AtomicInteger();
        FunctionMapFeature feature = FunctionMapFeature.map("map_value", value -> {
            calls.incrementAndGet();
            return "mapped:" + value;
        });
        Function<ReactorQLRecord, Publisher<?>> mapper = valueMapper("select map_value(this) v from test", feature);
        Assertions.assertFalse(mapper instanceof ScalarValueMapper);
        Publisher<?> result = mapper.apply(valueRecord("value"));
        Assertions.assertEquals(0, calls.get());
        StepVerifier.create(Flux.<Object>from(result)).expectNext("mapped:value").verifyComplete();
        Assertions.assertEquals(1, calls.get());

        mapper = valueMapper("select map_value(missing) v from test", feature);
        StepVerifier.create(Flux.from(mapper.apply(valueRecord(Collections.emptyMap())))).verifyComplete();
        Assertions.assertEquals(1, calls.get());
        feature.defaultValue("fallback");
        StepVerifier.create(Flux.<Object>from(mapper.apply(valueRecord(Collections.emptyMap()))))
                    .expectNext("mapped:fallback").verifyComplete();
        Assertions.assertEquals(2, calls.get());

        RuntimeException failure = new RuntimeException("mapping failed");
        FunctionMapFeature throwing = FunctionMapFeature.map("map_error", value -> { throw failure; });
        Publisher<?> error = valueMapper("select map_error(this) v from test", throwing).apply(valueRecord(1));
        StepVerifier.create(Flux.from(error)).expectErrorMatches(actual -> actual == failure).verify();
        FunctionMapFeature nullMapping = FunctionMapFeature.map("map_null", value -> null);
        Publisher<?> nullResult = valueMapper("select map_null(this) v from test", nullMapping).apply(valueRecord(1));
        StepVerifier.create(Flux.from(nullResult)).expectError(NullPointerException.class).verify();
    }

    @Test
    void testValueMappingHonorsPublicPublisherMapperMutationAfterCompilation() {
        AtomicInteger originalCalls = new AtomicInteger();
        FunctionMapFeature feature = FunctionMapFeature.map("mutable_map", value -> {
            originalCalls.incrementAndGet();
            return "mapped:" + value;
        });
        Function<ReactorQLRecord, Publisher<?>> mapper = valueMapper("select mutable_map(this) v from test", feature);
        Function<Flux<Object>, Publisher<Object>> original = feature.mapper;
        feature.mapper = values -> values.concatWith(Flux.just("tail"));
        StepVerifier.create(Flux.<Object>from(mapper.apply(valueRecord("value"))))
                    .expectNext("value", "tail").verifyComplete();
        Assertions.assertEquals(0, originalCalls.get());
        feature.mapper = original;
        StepVerifier.create(Flux.<Object>from(mapper.apply(valueRecord("value"))))
                    .expectNext("mapped:value").verifyComplete();
        Assertions.assertEquals(1, originalCalls.get());
    }

    @Test
    void testDateFieldsKeepArgumentGuardMissingValuesAndMultiValueModifiers() {
        String[] functions = {"year", "month", "day_of_month", "day_of_year", "day_of_week", "hour", "minute", "second"};
        for (String function : functions) {
            Assertions.assertThrows(UnsupportedOperationException.class,
                                    () -> ReactorQL.builder().sql("select " + function + "() v from test").build());
            Assertions.assertThrows(UnsupportedOperationException.class,
                                    () -> ReactorQL.builder().sql("select " + function + "(this,this) v from test").build());
            StepVerifier.create(ReactorQL.builder().sql("select " + function + "(missing) v from test").build()
                                          .start(Flux.just(Collections.emptyMap())))
                        .expectNext(Collections.emptyMap()).verifyComplete();
        }
        LocalDateTime first = LocalDateTime.of(2024, 2, 29, 3, 4, 5);
        LocalDateTime second = first.plusYears(1);
        FunctionMapFeature dates = new FunctionMapFeature("date_values", 1, 1,
                                                         values -> Flux.just(first, second, first));
        StepVerifier.create(Flux.<Object>from(valueMapper("select year(date_values(this)) v from test", dates)
                                              .apply(valueRecord(1))))
                    .expectNext(2024, 2025, 2024).verifyComplete();
        StepVerifier.create(Flux.<Object>from(valueMapper("select year(distinct date_values(this)) v from test", dates)
                                              .apply(valueRecord(1))))
                    .expectNext(2024, 2025).verifyComplete();
        StepVerifier.create(Flux.<Object>from(valueMapper("select year(unique date_values(this)) v from test", dates)
                                              .apply(valueRecord(1))))
                    .expectNext(2025).verifyComplete();
    }

    @Test
    void testDateFieldPublisherArgumentKeepsContextDemandErrorAndCancellation() {
        RuntimeException failure = new RuntimeException("date source failed");
        AtomicInteger calls = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        FunctionMapFeature dates = new FunctionMapFeature("date_signal", 1, 1, values -> values.concatMap(value ->
                Flux.deferContextual(context -> {
                    Assertions.assertEquals("visible", context.get("marker"));
                    calls.incrementAndGet();
                    if ("error".equals(value)) {
                        return Flux.error(failure);
                    }
                    if ("empty".equals(value)) {
                        return Flux.empty();
                    }
                    return Flux.just(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2025, 1, 1, 0, 0))
                               .concatWith(Flux.never()).doOnCancel(() -> cancelled.set(true));
                })));
        Function<ReactorQLRecord, Publisher<?>> mapper = valueMapper("select year(date_signal(this)) v from test", dates);
        Flux<Object> source = Flux.<Object>from(mapper.apply(valueRecord("stream")))
                             .contextWrite(context -> context.put("marker", "visible"));
        Assertions.assertEquals(0, calls.get());
        StepVerifier.create(source, 0).thenRequest(1).expectNext(2024)
                    .thenRequest(1).expectNext(2025).thenCancel().verify();
        Assertions.assertTrue(cancelled.get());
        Assertions.assertEquals(1, calls.get());
        StepVerifier.create(Flux.from(mapper.apply(valueRecord("error")))
                                .contextWrite(context -> context.put("marker", "visible")))
                    .expectErrorMatches(actual -> actual == failure).verify();
        StepVerifier.create(Flux.from(mapper.apply(valueRecord("empty")))
                                .contextWrite(context -> context.put("marker", "visible"))).verifyComplete();
    }

    @Test
    void testDateFieldRetainsMixedProjectionErrorCombination() {
        RuntimeException failure = new RuntimeException("first column failed");
        AtomicInteger subscriptions = new AtomicInteger();
        FunctionMapFeature first = new FunctionMapFeature("first_error", 1, 1, values -> Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.error(failure);
        }));
        Flux<Map<String, Object>> result = ReactorQL.builder().feature(first)
                                                   .sql("select first_error(this) first_value,year('not a date') date_value from test")
                                                   .build().start(Flux.just(1));
        Assertions.assertEquals(0, subscriptions.get());
        StepVerifier.create(result).expectErrorSatisfies(error -> {
            Assertions.assertTrue(Exceptions.isMultiple(error));
            // Reactor adds diagnostic tracebacks to suppressed errors, alongside column failures.
            List<Throwable> errors = Exceptions.unwrapMultipleExcludingTracebacks(error);
            Assertions.assertEquals(2, errors.size());
            Assertions.assertTrue(errors.contains(failure));
        }).verify();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void testValueMappingKeepsLegacyOnErrorContinueAtTheValueBoundary() {
        FunctionMapFeature legacy = new FunctionMapFeature("legacy_year", 1, 1,
                values -> values.map(value -> CastUtils.castLocalDateTime(value).getYear()));
        LocalDateTime first = LocalDateTime.of(2024, 1, 1, 0, 0);
        List<Map<String, Object>> expected = ReactorQL.builder().feature(legacy)
                .sql("select legacy_year(this) value from test").build()
                .start(Flux.just(first, "not a date", first.plusYears(1)))
                .onErrorContinue((error, value) -> { }).collectList().block();
        List<Map<String, Object>> actual = ReactorQL.builder()
                .sql("select year(this) value from test").build()
                .start(Flux.just(first, "not a date", first.plusYears(1)))
                .onErrorContinue((error, value) -> { }).collectList().block();
        Assertions.assertEquals(expected, actual);
    }

    private static Function<ReactorQLRecord, Publisher<?>> valueMapper(String sql, ValueMapFeature feature) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        metadata.addFeature(feature);
        net.sf.jsqlparser.statement.select.SelectExpressionItem item =
                (net.sf.jsqlparser.statement.select.SelectExpressionItem) metadata.getSql().getSelectItems().get(0);
        return ValueMapFeature.createMapperNow(item.getExpression(), metadata);
    }

    private static ReactorQLRecord valueRecord(Object value) {
        return new DefaultReactorQLRecord("test", value, new DefaultReactorQLContext(ignored -> Flux.empty()));
    }

    @Test
    void testLegacyProtectedApplyOverrideIsPreserved() {
        LegacyApplyFeature feature = new LegacyApplyFeature();

        ReactorQL
                .builder()
                .feature(feature)
                .sql("select legacy_apply(this) v from test")
                .build()
                .start(Flux.just("value"))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals("legacy:value", result.get("v")))
                .verifyComplete();

        Assertions.assertEquals(1, feature.applyCount.get());
    }

    @Test
    void testMetadataAwareMapperReceivesMetadataForParameterFunctions() {
        FunctionMapFeature feature = new FunctionMapFeature(
                "metadata_echo",
                1,
                1,
                (metadata, values) -> values.map(value -> String.valueOf(metadata.getSetting("prefix").orElse("missing")) + ":" + value)
        );

        ReactorQL
                .builder()
                .setting("prefix", "p")
                .feature(feature)
                .sql("select metadata_echo(this) v from test")
                .build()
                .start(Flux.just("value"))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals("p:value", result.get("v")))
                .verifyComplete();
    }

    @Test
    void testNoParameterAndDefaultValueBranchesRemainUsable() {
        FunctionMapFeature constant = new FunctionMapFeature(
                "legacy_const",
                0,
                0,
                values -> values.defaultIfEmpty("empty").next()
        );
        FunctionMapFeature metadataConstant = new FunctionMapFeature(
                "metadata_const",
                0,
                0,
                (metadata, values) -> Mono.just(metadata.getSetting("marker").orElse("missing"))
        );
        FunctionMapFeature collectWithDefault = new FunctionMapFeature(
                "collect_defaults",
                2,
                2,
                Flux::collectList
        ).defaultValue("fallback");

        ReactorQL
                .builder()
                .setting("marker", "ok")
                .feature(constant, metadataConstant, collectWithDefault)
                .sql("select legacy_const() legacyVal, metadata_const() metadataVal, collect_defaults(present, missing) defaultsVal from test")
                .build()
                .start(Flux.just(Collections.singletonMap("present", "value")))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals("empty", result.get("legacyVal"));
                    Assertions.assertEquals("ok", result.get("metadataVal"));
                    Assertions.assertEquals(Arrays.asList("value", "fallback"), result.get("defaultsVal"));
                })
                .verifyComplete();
    }

    @Test
    void testParameterGuardAndDistinctUniqueWrappers() {
        FunctionMapFeature emitValues = new FunctionMapFeature(
                "emit_values",
                3,
                1,
                values -> values
        );
        FunctionMapFeature needArg = new FunctionMapFeature(
                "need_arg",
                1,
                1,
                Flux::next
        );

        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .feature(needArg)
                .sql("select need_arg() v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .feature(needArg)
                .sql("select need_arg(1, 2) v from dual")
                .build());

        ReactorQL
                .builder()
                .feature(emitValues)
                .sql("select emit_values(distinct a, b, c) distinctValue, emit_values(unique a, b, c) uniqueValue from test")
                .build()
                .start(Flux.just(row(1, 2, 1)))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(1, result.get("distinctValue"));
                    Assertions.assertEquals(2, result.get("uniqueValue"));
                })
                .verifyComplete();
    }

    @Test
    void testParameterStreamPreservesSqlArgumentOrderForAsyncMappers() {
        FunctionMapFeature asyncValue = new FunctionMapFeature(
                "async_value",
                1,
                1,
                values -> values
                        .next()
                        .flatMap(value -> Mono
                                .delay(Duration.ofMillis((3L - ((Number) value).longValue()) * 10L))
                                .map(ignore -> value))
        );
        FunctionMapFeature collectArgs = new FunctionMapFeature(
                "collect_args",
                3,
                1,
                Flux::collectList
        );

        ReactorQL
                .builder()
                .feature(asyncValue, collectArgs)
                .sql("select collect_args(async_value(1), async_value(2), async_value(3)) values from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals(Arrays.asList(1L, 2L, 3L), result.get("values")))
                .verifyComplete();
    }

    @Test
    void testSingleParameterPublisherRemainsColdAndKeepsAllValuesAndContext() {
        AtomicInteger calls = new AtomicInteger();
        ValueMapFeature argument = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> {
                    calls.incrementAndGet();
                    return Flux.deferContextual(view -> Flux.just(view.get("marker"), record.getRecord()));
                };
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_arg").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                .feature(argument, new FunctionMapFeature("collect_arg", 1, 1, Flux::collectList))
                .sql("select collect_arg(cold_arg(this)) value from test")
                .build();
        Flux<Map<String, Object>> result = query.start(Flux.just(7));
        Assertions.assertEquals(0, calls.get());
        StepVerifier.create(result.contextWrite(context -> context.put("marker", "visible")))
                    .expectNext(Collections.singletonMap("value", Arrays.asList("visible", 7)))
                    .verifyComplete();
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void testMultiplePublisherParametersStayColdOrderedAndContextual() {
        AtomicInteger firstCalls = new AtomicInteger();
        AtomicInteger secondCalls = new AtomicInteger();
        AtomicBoolean firstCompleted = new AtomicBoolean();
        ValueMapFeature first = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> {
                    firstCalls.incrementAndGet();
                    return Flux.deferContextual(view -> Flux.just(
                            view.get("marker") + ":first-1",
                            view.get("marker") + ":first-2"))
                               .doOnComplete(() -> firstCompleted.set(true));
                };
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("first_arg").getId();
            }
        };
        ValueMapFeature second = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> {
                    Assertions.assertTrue(firstCompleted.get());
                    secondCalls.incrementAndGet();
                    return Mono.deferContextual(view -> Mono.just(view.get("marker") + ":second"));
                };
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("second_arg").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(first, second,
                                            new FunctionMapFeature("collect_ordered", 2, 2, Flux::collectList))
                                   .sql("select collect_ordered(first_arg(this),second_arg(this)) value from test")
                                   .build();
        Flux<Map<String, Object>> result = query.start(Flux.just(1));
        Assertions.assertEquals(0, firstCalls.get());
        Assertions.assertEquals(0, secondCalls.get());

        StepVerifier.create(result.contextWrite(context -> context.put("marker", "seen")), 0)
                    .thenRequest(1)
                    .expectNext(Collections.singletonMap(
                            "value", Arrays.asList("seen:first-1", "seen:first-2", "seen:second")))
                    .verifyComplete();
        Assertions.assertEquals(1, firstCalls.get());
        Assertions.assertEquals(1, secondCalls.get());
    }

    @Test
    void testMultiplePublisherParametersStopOnErrorAndCancellation() {
        RuntimeException failure = new RuntimeException("first parameter failed");
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger secondCalls = new AtomicInteger();
        ValueMapFeature first = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> "error".equals(record.getRecord())
                        ? Flux.error(failure)
                        : Flux.never()
                              .doOnSubscribe(ignore -> subscribed.set(true))
                              .doOnCancel(() -> cancelled.set(true));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("first_signal").getId();
            }
        };
        ValueMapFeature second = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> {
                    secondCalls.incrementAndGet();
                    return Mono.just("second");
                };
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("second_signal").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(first, second,
                                            new FunctionMapFeature("collect_signals", 2, 2, Flux::collectList))
                                   .sql("select collect_signals(first_signal(this),second_signal(this)) value from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just("error")))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
        Assertions.assertEquals(0, secondCalls.get());
        StepVerifier.create(query.start(Flux.just("never")), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
        Assertions.assertEquals(0, secondCalls.get());
    }

    @Test
    void testSingleParameterDefaultErrorAndCancellation() {
        FunctionMapFeature defaultOne = new FunctionMapFeature(
                "default_one", 1, 1, Flux::next).defaultValue("fallback");
        StepVerifier.create(ReactorQL.builder()
                                    .feature(defaultOne)
                                    .sql("select default_one(missing) value from test")
                                    .build()
                                    .start(Flux.just(Collections.emptyMap())))
                    .expectNext(Collections.singletonMap("value", "fallback"))
                    .verifyComplete();

        RuntimeException failure = new RuntimeException("argument failed");
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature argument = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> record.getRecord().equals("error")
                        ? Flux.error(failure)
                        : Flux.never()
                                .doOnSubscribe(ignore -> subscribed.set(true))
                                .doOnCancel(() -> cancelled.set(true));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("signal_arg").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                .feature(argument, new FunctionMapFeature("collect_signal", 1, 1, Flux::collectList))
                .sql("select collect_signal(signal_arg(this)) value from test")
                .build();
        StepVerifier.create(query.start(Flux.just("error")))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
        StepVerifier.create(query.start(Flux.just("never")), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void testScalarFunctionArgumentsRemainMutable() {
        FunctionMapFeature mutableArguments = FunctionMapFeature.scalar(
                "mutable_args",
                3,
                0,
                values -> {
                    values.add("tail");
                    values.add(0, "head");
                    values.set(0, "changed");
                    values.remove(0);
                    int size = values.size();
                    values.clear();
                    values.add(size);
                    return values.get(0);
                }
        );

        ReactorQL
                .builder()
                .feature(mutableArguments)
                .sql("select mutable_args() zero, mutable_args(1, 2, 3) three from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(1, result.get("zero"));
                    Assertions.assertEquals(4, result.get("three"));
                })
                .verifyComplete();
    }

    @Test
    void testBinaryScalarFunctionDefaultAndMissingArguments() {
        FunctionMapFeature direct = FunctionMapFeature.scalar2(
                "binary_direct", (first, second) -> first + ":" + second
        ).defaultValue("fallback");
        FunctionMapFeature legacy = FunctionMapFeature.scalar(
                "binary_legacy", 2, 2, values -> values.get(0) + ":" + values.get(1)
        ).defaultValue("fallback");

        ReactorQL query = ReactorQL.builder()
                                   .feature(direct, legacy)
                                   .sql("select binary_direct(a,b) direct_value, binary_legacy(a,b) legacy_value from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just(
                Collections.singletonMap("a", "left"),
                row("left", "right", null)
        )))
                    .assertNext(result -> {
                        Assertions.assertEquals("left:fallback", result.get("direct_value"));
                        Assertions.assertEquals(result.get("legacy_value"), result.get("direct_value"));
                    })
                    .assertNext(result -> {
                        Assertions.assertEquals("left:right", result.get("direct_value"));
                        Assertions.assertEquals(result.get("legacy_value"), result.get("direct_value"));
                    })
                    .verifyComplete();

        FunctionMapFeature missing = FunctionMapFeature.scalar2("binary_missing", (first, second) -> first + ":" + second);
        StepVerifier.create(ReactorQL.builder()
                                    .feature(missing)
                                    .sql("select binary_missing(a,b) value from test")
                                    .build()
                                    .start(Flux.just(Collections.singletonMap("a", "left"))))
                    .expectError(IndexOutOfBoundsException.class)
                    .verify();
    }

    @Test
    void testScalarFactoryHonorsPublicMapperReplacement() {
        FunctionMapFeature feature = FunctionMapFeature.scalar2("replace_mapper", (first, second) -> first + ":" + second);
        ReactorQL query = ReactorQL.builder().feature(feature)
                                  .sql("select replace_mapper(a,b) value from test").build();
        Function<Flux<Object>, Publisher<Object>> original = feature.mapper;
        StepVerifier.create(query.start(Flux.just(row("left", "right", null))))
                    .expectNext(Collections.singletonMap("value", "left:right"))
                    .verifyComplete();
        feature.mapper = stream -> stream.then(Mono.just("replacement"));
        StepVerifier.create(query.start(Flux.just(row("left", "right", null))))
                    .expectNext(Collections.singletonMap("value", "replacement"))
                    .verifyComplete();
        feature.mapper = original;
        StepVerifier.create(query.start(Flux.just(row("left", "right", null))))
                    .expectNext(Collections.singletonMap("value", "left:right"))
                    .verifyComplete();
    }

    @Test
    void testBinaryScalarFunctionKeepsAsyncContextAndCancellation() {
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature asyncArgument = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> record.getRecord().equals("never")
                        ? Flux.never()
                              .doOnSubscribe(ignore -> subscribed.set(true))
                              .doOnCancel(() -> cancelled.set(true))
                        : Mono.deferContextual(view -> Mono.just(view.get("marker")));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("binary_async_arg").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(asyncArgument,
                                            FunctionMapFeature.scalar2("binary_async", (first, second) -> first + ":" + second))
                                   .sql("select binary_async(binary_async_arg(this), 'tail') value from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just("value"))
                                 .contextWrite(context -> context.put("marker", "visible")))
                    .expectNext(Collections.singletonMap("value", "visible:tail"))
                    .verifyComplete();
        StepVerifier.create(query.start(Flux.just("never")), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void testTernaryScalarFunctionKeepsDefaultMissingAndOptionalArgumentSemantics() {
        FunctionMapFeature direct = FunctionMapFeature.scalar(
                "ternary_direct", 3, 3,
                (metadata, values) -> values.get(0) + ":" + values.get(1) + ":" + values.get(2)
        ).defaultValue("fallback");
        FunctionMapFeature legacy = FunctionMapFeature.scalar(
                "ternary_legacy", 3, 3,
                values -> values.get(0) + ":" + values.get(1) + ":" + values.get(2)
        ).defaultValue("fallback");
        ReactorQL query = ReactorQL.builder()
                                   .feature(direct, legacy)
                                   .sql("select ternary_direct(a,b,c) direct_value, ternary_legacy(a,b,c) legacy_value from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just(row("left", null, "right"), row("left", "middle", "right"))))
                    .assertNext(result -> {
                        Assertions.assertEquals("left:fallback:right", result.get("direct_value"));
                        Assertions.assertEquals(result.get("legacy_value"), result.get("direct_value"));
                    })
                    .assertNext(result -> {
                        Assertions.assertEquals("left:middle:right", result.get("direct_value"));
                        Assertions.assertEquals(result.get("legacy_value"), result.get("direct_value"));
                    })
                    .verifyComplete();

        FunctionMapFeature missing = FunctionMapFeature.scalar(
                "ternary_missing", 3, 3,
                (metadata, values) -> values.get(2)
        );
        StepVerifier.create(ReactorQL.builder()
                                    .feature(missing)
                                    .sql("select ternary_missing(a,b,c) value from test")
                                    .build()
                                    .start(Flux.just(row("left", "middle", null))))
                    .expectError(IndexOutOfBoundsException.class)
                    .verify();

        FunctionMapFeature optional = FunctionMapFeature.scalar(
                "ternary_optional", 3, 2,
                (metadata, values) -> values.size() == 2 ? "two" : "three"
        );
        StepVerifier.create(ReactorQL.builder()
                                    .feature(optional)
                                    .sql("select ternary_optional(a,b) two, ternary_optional(a,b,c) three from test")
                                    .build()
                                    .start(Flux.just(row("a", "b", "c"))))
                    .assertNext(result -> {
                        Assertions.assertEquals("two", result.get("two"));
                        Assertions.assertEquals("three", result.get("three"));
                    })
                    .verifyComplete();
    }

    @Test
    void testTernaryScalarFunctionKeepsAsyncContextAndCancellation() {
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature asyncArgument = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    org.jetlinks.reactor.ql.ReactorQLMetadata metadata) {
                return record -> record.getRecord().equals("never")
                        ? Flux.never()
                              .doOnSubscribe(ignore -> subscribed.set(true))
                              .doOnCancel(() -> cancelled.set(true))
                        : Mono.deferContextual(view -> Mono.just(view.get("marker")));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("ternary_async_arg").getId();
            }
        };
        FunctionMapFeature function = FunctionMapFeature.scalar(
                "ternary_async", 3, 3,
                (metadata, values) -> values.get(0) + ":" + values.get(1) + ":" + values.get(2)
        );
        ReactorQL query = ReactorQL.builder()
                                   .feature(asyncArgument, function)
                                   .sql("select ternary_async(ternary_async_arg(this), 'middle', 'tail') value from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just("value"))
                                 .contextWrite(context -> context.put("marker", "visible")))
                    .expectNext(Collections.singletonMap("value", "visible:middle:tail"))
                    .verifyComplete();
        StepVerifier.create(query.start(Flux.just("never")), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    private static Map<String, Object> row(Object a, Object b, Object c) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("a", a);
        row.put("b", b);
        row.put("c", c);
        return row;
    }

    /** Constant scalar source; native Mono handles its request/cancel protocol, counters observe adaptation. */
    private static final class CallableValuePublisher implements Publisher<Object>, Fuseable.ScalarCallable<Object> {

        private final Object value;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger subscriptions = new AtomicInteger();

        private CallableValuePublisher(Object value) {
            this.value = value;
        }

        @Override
        public Object call() {
            calls.incrementAndGet();
            return value;
        }

        @Override
        public void subscribe(Subscriber<? super Object> subscriber) {
            subscriptions.incrementAndGet();
            Mono.fromCallable(this).subscribe(subscriber);
        }
    }

    private static class LegacyApplyFeature extends FunctionMapFeature {

        private final AtomicInteger applyCount = new AtomicInteger();

        private LegacyApplyFeature() {
            super("legacy_apply", 1, 1, values -> Mono.error(new IllegalStateException("legacy apply override was not used")));
        }

        @Override
        protected Publisher<Object> apply(ReactorQLRecord record,
                                          List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
            applyCount.incrementAndGet();
            return Mono
                    .fromDirect(mappers.get(0).apply(record))
                    .map(value -> "legacy:" + value);
        }
    }
}
