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
import org.jetlinks.reactor.ql.internal.FirstValueMapper;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;

/** Public predicate factories retain scalar, raw-row and Publisher extension contracts. */
class FilterFeatureCompatibilityTest {

    @Test
    void publisherLikePreparesInvalidConstantsOnlyAfterBothValues() {
        for (boolean checkpoint : Arrays.asList(false, true)) {
            for (String operator : Arrays.asList("like", "not like")) {
                AtomicBoolean present = new AtomicBoolean();
                DefaultReactorQLMetadata metadata = metadata("left_signal() " + operator + " '['",
                        feature(FeatureId.ValueMap.of("left_signal").getId(),
                                row -> Mono.defer(() -> present.get() ? Mono.just("value") : Mono.empty())));
                metadata.setting("checkpoint", checkpoint);
                BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata);
                StepVerifier.create(predicate.apply(record(row(true)), true)).verifyComplete();
                present.set(true);
                Mono<Boolean> result = Assertions.assertDoesNotThrow(() -> predicate.apply(record(row(true)), true));
                StepVerifier.create(result).expectError(PatternSyntaxException.class).verify();
                present.set(false);
                StepVerifier.create(predicate.apply(record(row(true)), true)).verifyComplete();
                ReactorQL query = ReactorQL.builder().setting("checkpoint", checkpoint)
                        .sql("select * from test where lower(flag) " + operator + " '['").build();
                StepVerifier.create(query.start(Flux.empty())).verifyComplete();
            }
        }
    }

    @Test
    void publisherLikeConstantKeepsContextFirstValueAndOperandEvaluation() {
        AtomicInteger emissions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        AtomicInteger rightCalls = new AtomicInteger();
        ScalarValueMapper right = new ScalarValueMapper() {
            @Override
            public Object applyScalar(ReactorQLRecord row) {
                rightCalls.incrementAndGet();
                return "val%";
            }

            @Override
            public boolean isConstant() { return true; }

            @Override
            public Object constantValue() { return "val%"; }
        };
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(
                "left_signal() like right_signal()",
                feature(FeatureId.ValueMap.of("left_signal").getId(), row -> Flux.deferContextual(context -> {
                    Assertions.assertEquals("available", context.get(CONTEXT_KEY));
                    return Flux.just("value", "different");
                }).doOnNext(value -> emissions.incrementAndGet()).doOnCancel(cancellations::incrementAndGet)),
                feature(FeatureId.ValueMap.of("right_signal").getId(), right)));
        for (int repeat = 0; repeat < 2; repeat++) {
            StepVerifier.create(predicate.apply(record(row(true)), true)
                    .contextWrite(context -> context.put(CONTEXT_KEY, "available")), 0)
                    .thenRequest(1).expectNext(true).verifyComplete();
        }
        Assertions.assertEquals(2, emissions.get());
        Assertions.assertEquals(2, cancellations.get());
        Assertions.assertEquals(2, rightCalls.get());

        TestPublisher<String> pending = TestPublisher.create();
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> cancellable = predicate(metadata(
                "left_signal() like 'val%'",
                feature(FeatureId.ValueMap.of("left_signal").getId(), row -> pending.flux())));
        StepVerifier.create(cancellable.apply(record(row(true)), true)).thenCancel().verify();
        pending.assertCancelled();
    }

    @Test
    void publisherLikeKeepsDynamicMutableAndFirstValuePatterns() {
        AtomicBoolean matching = new AtomicBoolean();
        ScalarValueMapper dynamic = row -> matching.get() ? "val%" : "miss%";
        StringBuilder mutable = new StringBuilder("miss%");
        class ConstantWithFirstValue implements ScalarValueMapper, FirstValueMapper {
            @Override
            public Object applyScalar(ReactorQLRecord row) { return "val%"; }

            @Override
            public boolean isConstant() { return true; }

            @Override
            public Object constantValue() { return "val%"; }

            @Override
            public Mono<?> first(ReactorQLRecord row) { return Mono.just(dynamic.applyScalar(row)); }
        }
        for (ScalarValueMapper right : Arrays.asList(dynamic, ScalarValueMapper.constant(mutable),
                                                     new ConstantWithFirstValue())) {
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(
                    "left_signal() like right_signal()",
                    feature(FeatureId.ValueMap.of("left_signal").getId(), row -> Mono.just("value")),
                    feature(FeatureId.ValueMap.of("right_signal").getId(), right)));
            StepVerifier.create(predicate.apply(record(row(true)), true)).expectNext(false).verifyComplete();
            matching.set(true);
            mutable.replace(0, mutable.length(), "val%");
            StepVerifier.create(predicate.apply(record(row(true)), true)).expectNext(true).verifyComplete();
            matching.set(false);
            mutable.replace(0, mutable.length(), "miss%");
        }
    }

    @Test
    void publisherLikeKeepsZipErrorHookDataAndNonResumableFailure() {
        AtomicInteger hooks = new AtomicInteger();
        AtomicInteger continued = new AtomicInteger();
        Hooks.onOperatorError("prepared-like-test", (error, value) -> {
            if (error instanceof PatternSyntaxException) {
                Assertions.assertArrayEquals(new Object[]{"value", "["}, (Object[]) value);
                hooks.incrementAndGet();
            }
            return error;
        });
        try {
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(
                    "left_signal() like '['",
                    feature(FeatureId.ValueMap.of("left_signal").getId(), row -> Mono.just("value"))));
            StepVerifier.create(predicate.apply(record(row(true)), true)
                    .onErrorContinue((error, value) -> continued.incrementAndGet()))
                    .expectError(PatternSyntaxException.class).verify();
            Assertions.assertEquals(1, hooks.get());
            Assertions.assertEquals(0, continued.get());
        } finally {
            Hooks.resetOnOperatorError("prepared-like-test");
        }
    }

    @Test
    void binaryPublisherPredicatesKeepDemandContextAndBothSubscriptions() {
        for (String condition : Arrays.asList(
                "left_signal() = right_signal()",
                "left_signal() like right_signal()",
                "left_signal() = 'value' and right_signal() = 'value'",
                "left_signal() = 'value' or right_signal() = 'value'")) {
            AtomicInteger subscriptions = new AtomicInteger();
            Function<ReactorQLRecord, Publisher<?>> source = record -> Mono.deferContextual(context -> {
                Assertions.assertEquals("available", context.get(CONTEXT_KEY));
                subscriptions.incrementAndGet();
                return Mono.just("value");
            });
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(condition,
                    feature(FeatureId.ValueMap.of("left_signal").getId(), source),
                    feature(FeatureId.ValueMap.of("right_signal").getId(), source)));
            for (int repeat = 0; repeat < 2; repeat++) {
                Mono<Boolean> result = predicate.apply(record(row(true)), true);
                int before = subscriptions.get();
                Assertions.assertEquals(repeat * 2, before, condition + " remains cold before subscription");
                StepVerifier.create(result.contextWrite(ctx -> ctx.put(CONTEXT_KEY, "available")), 0)
                        .thenRequest(1).expectNext(true).verifyComplete();
                Assertions.assertEquals(before + 2, subscriptions.get(), condition);
            }
        }
    }

    @Test
    void binaryPublisherPredicatesCancelBothOperandsAndKeepOriginalErrors() {
        for (String condition : Arrays.asList(
                "left_signal() = right_signal()",
                "left_signal() like right_signal()",
                "left_signal() = 'value' and right_signal() = 'value'",
                "left_signal() = 'value' or right_signal() = 'value'")) {
            for (boolean fail : Arrays.asList(false, true)) {
                TestPublisher<String> left = TestPublisher.create();
                TestPublisher<String> right = TestPublisher.create();
                BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate = predicate(metadata(condition,
                        feature(FeatureId.ValueMap.of("left_signal").getId(), record -> left.mono()),
                        feature(FeatureId.ValueMap.of("right_signal").getId(), record -> right.mono())));
                RuntimeException failure = new RuntimeException("operand failure");
                StepVerifier.Step<Boolean> verifier = StepVerifier.create(predicate.apply(record(row(true)), true))
                        .then(() -> {
                            left.assertWasSubscribed();
                            right.assertWasSubscribed();
                        });
                if (fail) {
                    verifier.then(() -> left.error(failure))
                            .expectErrorMatches(error -> error == failure).verify();
                } else {
                    verifier.thenCancel().verify();
                    left.assertCancelled();
                }
                right.assertCancelled();
            }
        }
    }

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
        RawScalarValueMapper mapper = rawFlag();
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
            for (Object value : Arrays.asList(null, false, true)) {
                Map<String, Object> row = row(value);
                ReactorQLRecord record = record(row);
                boolean expected = expectedFunction(condition, value);
                Assertions.assertEquals(expected, raw.testRaw(row));
                Assertions.assertEquals(expected, raw.test(record, row));
                Assertions.assertEquals(expected, raw.recordFilter().test(record, row));
                assertResult(raw, record, row, expected);
            }
        }
    }

    @Test
    void checkpointAndMetadataExtensionsKeepRawFunctionsOnTheRecordPath() {
        ValueMapFeature feature = feature(SIGNAL, rawFlag());
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

    private static RawScalarValueMapper rawFlag() {
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
