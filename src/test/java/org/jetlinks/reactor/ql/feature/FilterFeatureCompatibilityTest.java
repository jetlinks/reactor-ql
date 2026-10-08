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
package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.DefaultReactorQLContext;
import org.jetlinks.reactor.ql.DefaultReactorQLRecord;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Public predicate factories retain scalar, raw-row and Publisher extension contracts. */
class FilterFeatureCompatibilityTest {

    private static final String SIGNAL = FeatureId.ValueMap.of("flag_signal").getId();
    private static final String CONTEXT_KEY = "filter-test-value";
    private static final Object[][] BOOLEAN_CONDITIONS = {
            {"flag", false, true},
            {"not flag", true, false},
            {"flag is true", false, true},
            {"flag is false", true, false},
            {"flag is not true", true, false},
            {"flag is not false", false, true}
    };

    @Test
    void scalarFunctionsKeepNullFalseAndTrueInBothPredicateViews() {
        ScalarValueMapper mapper = record -> flag(record.getRecord());
        for (String condition : Arrays.asList("flag_signal()", "flag_signal() is null", "flag_signal() is not null")) {
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(
                    metadata(condition, feature(SIGNAL, mapper)));
            Assertions.assertTrue(predicate instanceof ScalarFilter);
            Assertions.assertFalse(predicate instanceof RawScalarFilter);
            for (Object value : Arrays.asList(null, false, true)) {
                ReactorQLRecord record = record(row(value));
                boolean expected = expectedFunction(condition, value);
                Assertions.assertEquals(expected, ((ScalarFilter) predicate).test(record, record.getRecord()));
                assertResult(predicate, record, record.getRecord(), expected);
            }
        }
    }

    @Test
    void rawFunctionsAndNullChecksDelegateCapabilitiesAndKeepRecordEquivalence() {
        for (boolean anyRow : Arrays.asList(false, true)) {
            RawScalarValueMapper mapper = rawFlag(anyRow);
            for (String condition : Arrays.asList("flag_signal()", "flag_signal() is null",
                                                  "flag_signal() is not null")) {
                DefaultReactorQLMetadata metadata = metadata(condition, feature(SIGNAL, mapper));
                BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = FilterFeature
                        .createPredicateByExpression(metadata.getSql().getWhere(), metadata)
                        .orElseThrow(() -> new AssertionError("Missing predicate: " + condition));
                Assertions.assertTrue(predicate instanceof RawScalarFilter);
                RawScalarFilter raw = (RawScalarFilter) predicate;
                Assertions.assertTrue(raw.acceptsSource("t"));
                Assertions.assertFalse(raw.acceptsSource("other"));
                Assertions.assertFalse(raw.acceptsSource(null));
                Assertions.assertEquals(anyRow, raw.acceptsAnyRow());
                for (Object value : Arrays.asList(null, false, true)) {
                    Map<String, Object> row = row(value);
                    ReactorQLRecord record = record(row);
                    boolean expected = expectedFunction(condition, value);
                    Assertions.assertEquals(expected, raw.testRaw(row));
                    Assertions.assertEquals(expected, raw.test(record, row));
                    Assertions.assertEquals(expected, raw.recordFilter().test(record, row));
                    assertResult(raw, record, row, expected);
                }
                if (anyRow) {
                    Assertions.assertEquals(expectedFunction(condition, true), raw.testRaw(true));
                }
            }
        }
    }

    @Test
    void checkpointAndMetadataExtensionsKeepRawFunctionsOnTheRecordPath() {
        ValueMapFeature feature = feature(SIGNAL, rawFlag(false));
        for (String condition : Arrays.asList("flag_signal()", "flag_signal() is null")) {
            DefaultReactorQLMetadata checkpoint = metadata(condition, feature);
            checkpoint.setting("checkpoint", true);
            WrappingMetadata wrapped = new WrappingMetadata(condition);
            wrapped.addFeature(feature);
            for (DefaultReactorQLMetadata metadata : Arrays.asList(checkpoint, wrapped)) {
                BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata);
                Assertions.assertTrue(predicate instanceof ScalarFilter);
                Assertions.assertFalse(predicate instanceof RawScalarFilter);
                for (Object value : Arrays.asList(null, false, true)) {
                    assertResult(predicate, record(row(value)), row(value), expectedFunction(condition, value));
                }
            }
        }
        StepVerifier.create(ReactorQL.builder().feature(feature).setting("checkpoint", true)
                                    .sql("select t.flag flag from test t where flag_signal()")
                                    .build().start(Flux.just(row(false), row(true))))
                    .expectNext(row(true)).verifyComplete();
    }

    @Test
    void scalarColumnBooleanAndNotPredicatesKeepNullAsFalse() {
        for (Object[] condition : BOOLEAN_CONDITIONS) {
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata((String) condition[0]));
            Assertions.assertTrue(predicate instanceof ScalarFilter);
            assertResult(predicate, record(row(null)), null, false);
            assertResult(predicate, record(row(false)), false, (Boolean) condition[1]);
            assertResult(predicate, record(row(true)), true, (Boolean) condition[2]);
        }
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> scalarCase = predicate(metadata(
                "case when flag is not null then flag end",
                feature(FeatureId.ValueMap.caseWhen.getId(), (ScalarValueMapper) record -> flag(record.getRecord()))));
        Assertions.assertTrue(scalarCase instanceof ScalarFilter);
        for (Object value : Arrays.asList(null, false, true)) {
            assertResult(scalarCase, record(row(value)), value, Boolean.TRUE.equals(value));
        }
    }

    @Test
    void publisherFunctionColumnBooleanNotAndCaseStayColdAndPreserveContextAndEmpty() {
        for (Object[] condition : BOOLEAN_CONDITIONS) {
            assertColdPredicate((String) condition[0], false, (Boolean) condition[1]);
            assertColdPredicate((String) condition[0], true, (Boolean) condition[2]);
            assertColdPredicate((String) condition[0], null, null);
        }
        for (Object value : Arrays.asList(null, false, true)) {
            assertColdPredicate("flag_signal()", value, (Boolean) value);
            assertColdPredicate("not flag_signal()", value, value == null ? null : !((Boolean) value));
            assertColdPredicate("case when flag then flag_signal() end", value, Boolean.TRUE.equals(value));
        }
    }

    @Test
    void publisherNullChecksDistinguishEmptyFromMultiValueAndCancelAfterFirstValue() {
        for (boolean present : Arrays.asList(false, true)) {
            for (boolean not : Arrays.asList(false, true)) {
                AtomicInteger subscriptions = new AtomicInteger();
                AtomicInteger emitted = new AtomicInteger();
                AtomicBoolean cancelled = new AtomicBoolean();
                Flux<Integer> source = Flux.<Integer>deferContextual(context -> {
                    Assertions.assertEquals("available", context.get(CONTEXT_KEY));
                    subscriptions.incrementAndGet();
                    return present ? Flux.just(1, 2, 3) : Flux.empty();
                }).doOnNext(ignore -> emitted.incrementAndGet()).doOnCancel(() -> cancelled.set(true));
                BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(
                        "flag_signal() is " + (not ? "not null" : "null"), feature(SIGNAL, record -> source)));
                Mono<Boolean> result = predicate.apply(record(row(true)), true);
                Assertions.assertFalse(predicate instanceof ScalarFilter);
                Assertions.assertEquals(0, subscriptions.get());
                StepVerifier.create(result.contextWrite(context -> context.put(CONTEXT_KEY, "available")))
                            .expectNext(not ? present : !present).verifyComplete();
                Assertions.assertEquals(1, subscriptions.get());
                Assertions.assertEquals(present ? 1 : 0, emitted.get());
                Assertions.assertEquals(present, cancelled.get());
            }
        }
    }

    @Test
    void publisherPredicateErrorsKeepTheOriginalFailureAndSingleSubscription() {
        for (String condition : Arrays.asList("flag_signal()", "not flag_signal()", "flag is true",
                                              "case when flag then flag_signal() end", "flag_signal() is null")) {
            RuntimeException failure = new RuntimeException("value mapper failed");
            AtomicInteger subscriptions = new AtomicInteger();
            Function<ReactorQLRecord, Publisher<?>> mapper = record -> Mono.deferContextual(context -> {
                Assertions.assertEquals("available", context.get(CONTEXT_KEY));
                subscriptions.incrementAndGet();
                return Mono.error(failure);
            });
            String featureId = condition.contains("flag_signal") ? SIGNAL : FeatureId.ValueMap.property.getId();
            Mono<Boolean> result = predicate(metadata(condition, feature(featureId, mapper)))
                    .apply(record(row(true)), true);
            Assertions.assertEquals(0, subscriptions.get());
            StepVerifier.create(result.contextWrite(context -> context.put(CONTEXT_KEY, "available")))
                        .expectErrorSatisfies(actual -> Assertions.assertSame(failure, actual)).verify();
            Assertions.assertEquals(1, subscriptions.get());
        }
    }

    @Test
    void existsKeepsBuiltInSubqueryAndPublisherExtensionCardinality() {
        for (boolean extension : Arrays.asList(false, true)) {
            for (boolean present : Arrays.asList(false, true)) {
                for (boolean not : Arrays.asList(false, true)) {
                    AtomicInteger subscriptions = new AtomicInteger();
                    AtomicInteger emitted = new AtomicInteger();
                    AtomicBoolean cancelled = new AtomicBoolean();
                    Flux<Map<String, Object>> source = Flux.<Map<String, Object>>defer(() -> {
                        subscriptions.incrementAndGet();
                        return present ? Flux.just(row(true), row(false)) : Flux.empty();
                    }).doOnNext(ignore -> emitted.incrementAndGet()).doOnCancel(() -> cancelled.set(true));
                    DefaultReactorQLMetadata metadata = metadata((not ? "not " : "")
                            + "exists(select flag from lookup)");
                    if (extension) {
                        // A public subquery mapper may return a multi-value Publisher without the internal EXISTS SPI.
                        metadata.addFeature(feature(FeatureId.ValueMap.select.getId(), record -> source));
                    }
                    ReactorQLRecord record = new DefaultReactorQLRecord("t", row(true),
                            new DefaultReactorQLContext(name -> source));
                    Mono<Boolean> result = predicate(metadata).apply(record, true);
                    Assertions.assertEquals(0, subscriptions.get());
                    StepVerifier.create(result).expectNext(present != not).verifyComplete();
                    Assertions.assertEquals(1, subscriptions.get());
                    Assertions.assertEquals(present ? 1 : 0, emitted.get());
                    Assertions.assertEquals(present, cancelled.get());
                }
            }
        }
    }

    @Test
    void binaryValueMappingKeepsScalarComparisonAndMetadataWrapperFallback() {
        ValueMapFeature scalar = feature(FeatureId.ValueMap.of("||").getId(),
                                        (ScalarValueMapper) record -> flag(record.getRecord()));
        DefaultReactorQLMetadata metadata = metadata("flag || ''", scalar);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> direct = predicate(metadata);
        Assertions.assertTrue(direct instanceof ScalarFilter);
        assertResult(direct, record(row("ready")), "ready", true);
        assertResult(direct, record(row("ready")), "other", false);
        assertResult(direct, record(row(null)), "ready", false);

        WrappingMetadata wrapped = new WrappingMetadata("flag || ''");
        wrapped.addFeature(scalar);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> fallback = predicate(wrapped);
        Assertions.assertFalse(fallback instanceof ScalarFilter);
        Mono<Boolean> result = fallback.apply(record(row("ready")), "ready");
        Assertions.assertEquals(0, wrapped.subscriptions.get());
        StepVerifier.create(result.contextWrite(context -> context.put(CONTEXT_KEY, "available")))
                    .expectNext(true).verifyComplete();
        Assertions.assertEquals(1, wrapped.subscriptions.get());
        Assertions.assertEquals("flag || ''", wrapped.expression);

        metadata.setting("checkpoint", true);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> checkpoint = predicate(metadata);
        Assertions.assertFalse(checkpoint instanceof ScalarFilter);
        assertResult(checkpoint, record(row("ready")), "ready", true);
        StepVerifier.create(checkpoint.apply(record(row(null)), "ready")).verifyComplete();
    }

    @Test
    void unsupportedConditionKeepsStructuredDiagnostics() {
        DefaultReactorQLMetadata metadata = metadata("?");
        Expression expression = metadata.getSql().getWhere();
        Assertions.assertFalse(FilterFeature.createPredicateByExpression(expression, metadata).isPresent());
        ReactorQLException failure = Assertions.assertThrows(ReactorQLException.class,
                () -> FilterFeature.createPredicateNow(expression, metadata));
        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_CONDITION, failure.getI18nCode());
        Assertions.assertEquals("?", failure.getExpression());
    }

    private static void assertColdPredicate(String condition, Object value, Boolean expected) {
        AtomicInteger subscriptions = new AtomicInteger();
        Function<ReactorQLRecord, Publisher<?>> mapper = record -> Mono.deferContextual(context -> {
            Assertions.assertEquals("available", context.get(CONTEXT_KEY));
            subscriptions.incrementAndGet();
            return Mono.justOrEmpty(value);
        });
        String featureId = condition.contains("flag_signal") ? SIGNAL : FeatureId.ValueMap.property.getId();
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(condition, feature(featureId, mapper)));
        Assertions.assertFalse(predicate instanceof ScalarFilter);
        Mono<Boolean> result = predicate.apply(record(row(true)), true);
        Assertions.assertEquals(0, subscriptions.get());
        StepVerifier.FirstStep<Boolean> verifier = StepVerifier.create(
                result.contextWrite(context -> context.put(CONTEXT_KEY, "available")));
        if (expected == null) {
            verifier.verifyComplete();
        } else {
            verifier.expectNext(expected).verifyComplete();
        }
        Assertions.assertEquals(1, subscriptions.get());
    }

    private static boolean expectedFunction(String condition, Object value) {
        if (condition.endsWith("is not null")) {
            return value != null;
        }
        if (condition.endsWith("is null")) {
            return value == null;
        }
        return Boolean.TRUE.equals(value);
    }

    private static void assertResult(BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate,
                                     ReactorQLRecord record, Object value, boolean expected) {
        StepVerifier.create(predicate.apply(record, value)).expectNext(expected).verifyComplete();
    }

    private static DefaultReactorQLMetadata metadata(String condition, Feature... features) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select * from test t where " + condition);
        metadata.addFeature(features);
        return metadata;
    }

    private static BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate(DefaultReactorQLMetadata metadata) {
        return FilterFeature.createPredicateNow(metadata.getSql().getWhere(), metadata);
    }

    private static ReactorQLRecord record(Object value) {
        return new DefaultReactorQLRecord("t", value, new DefaultReactorQLContext(ignore -> Flux.empty()));
    }

    private static Map<String, Object> row(Object value) {
        return Collections.singletonMap("flag", value);
    }

    private static Object flag(Object value) {
        return value instanceof Map ? ((Map<?, ?>) value).get("flag") : value;
    }

    private static RawScalarValueMapper rawFlag(boolean anyRow) {
        return new RawScalarValueMapper() {
            @Override
            public Object applyScalar(ReactorQLRecord record) {
                return flag(record.getRecord());
            }

            @Override
            public boolean acceptsSource(String alias) {
                return "t".equals(alias);
            }

            @Override
            public boolean acceptsAnyRow() {
                return anyRow;
            }

            @Override
            public Object applyRaw(Object row) {
                return flag(row);
            }
        };
    }

    private static ValueMapFeature feature(String id, Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return id;
            }
        };
    }

    /** A query-local wrapper observes subscription and Reactor Context without changing values. */
    private static final class WrappingMetadata extends DefaultReactorQLMetadata {
        private final AtomicInteger subscriptions = new AtomicInteger();
        private String expression;

        private WrappingMetadata(String condition) {
            super("select * from test t where " + condition);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Publisher<? extends R>, R> Function<T, T> createWrapper(Object expression) {
            this.expression = String.valueOf(expression);
            return source -> (T) Mono.deferContextual(context -> {
                Assertions.assertEquals("available", context.get(CONTEXT_KEY));
                subscriptions.incrementAndGet();
                return Mono.from(source);
            });
        }
    }
}
