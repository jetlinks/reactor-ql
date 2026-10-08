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
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.exception.TypeCastException;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.map.PropertyMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Checks value-local error continuation against the retained Publisher execution. */
class ScalarAggregateErrorBoundaryTest {

    @Test
    void comparisonFailuresKeepIncomingValueHookAndTerminalScope() {
        for (String function : Arrays.asList("min", "max")) {
            for (String group : Arrays.asList("", " group by _window(2),type")) {
                for (int mode = 0; mode < 6; mode++) {
                    RuntimeException failure = new IllegalStateException("comparison failed");
                    FailedComparison first = new FailedComparison(failure);
                    FailedComparison second = new FailedComparison(failure);
                    Flux<?> source = mode < 2
                            ? Flux.just(new ComparisonRow(first), new ComparisonRow(second))
                            : Flux.just(comparisonRow(first), comparisonRow(second));
                    if (mode >= 4) {
                        ReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
                        source = source.map(value -> ReactorQLRecord.newRecord("upstream", value, context));
                    }
                    java.util.ArrayList<Object> hookedValues = new java.util.ArrayList<>();
                    AtomicInteger continued = new AtomicInteger();
                    Hooks.onOperatorError("comparison-value-boundary", (error, value) -> {
                        hookedValues.add(value);
                        return error;
                    });
                    try {
                        ReactorQL query = ReactorQL.builder()
                                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, (mode & 1) != 0)
                                .sql("select " + function + "(score) result from test" + group).build();
                        Flux<Map<String, Object>> result = query.start(source);
                        // Global reduce has no resumable enclosing group. Window compatibility
                        // flatMap has a separate inner-error scope, exercised by the oracle below.
                        if (group.isEmpty()) {
                            result = result.onErrorContinue((error, value) -> continued.incrementAndGet());
                        }
                        StepVerifier.create(result, 0)
                                .thenRequest(1).expectErrorMatches(error -> error == failure).verify();
                        Assertions.assertEquals(java.util.Collections.singletonList(second), hookedValues);
                        Assertions.assertEquals(0, continued.get(), "Comparison reduction must stay terminal");
                    } finally {
                        Hooks.resetOnOperatorError("comparison-value-boundary");
                    }
                }
            }
        }
    }

    @Test
    void compatibilityWindowComparisonFailureUsesEnclosingInnerErrorScope() {
        RuntimeException failure = new IllegalStateException("comparison failed");
        AtomicInteger continued = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql("select max(score) result from test group by _window(2),type").build();
        StepVerifier.create(query.start(Flux.just(new ComparisonRow(new FailedComparison(failure)),
                        new ComparisonRow(new FailedComparison(failure))))
                        .onErrorContinue((error, value) -> {
                            Assertions.assertSame(failure, error);
                            Assertions.assertNull(value);
                            continued.incrementAndGet();
                        })).verifyComplete();
        Assertions.assertEquals(1, continued.get());
    }

    @Test
    void compatibilityMultiAggregateKeepsHealthyReducerAfterComparisonFailure() {
        RuntimeException failure = new IllegalStateException("comparison failed");
        AtomicInteger continued = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql("select max(score) highest,count(1) total from test").build();
        StepVerifier.create(query.start(Flux.just(
                        comparisonRow(new FailedComparison(failure)),
                        comparisonRow(new FailedComparison(failure)),
                        comparisonRow(3), comparisonRow(4))
                        .hide().doOnNext(ignore -> consumed.incrementAndGet()))
                        .onErrorContinue((error, value) -> {
                            Assertions.assertSame(failure, error);
                            Assertions.assertNull(value);
                            continued.incrementAndGet();
                        }), 0)
                .thenRequest(1)
                .expectNext(java.util.Collections.singletonMap("total", 4L))
                .verifyComplete();
        Assertions.assertEquals(1, continued.get());
        Assertions.assertEquals(4, consumed.get());
    }

    @Test
    void compatibilityMergeReadsGlobalErrorPolicyAtFailureTime() {
        RuntimeException failure = new IllegalStateException("comparison failed");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger continued = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql("select max(score) highest from test group by _window(2)").build();
        try {
            // A public Hook can change after subscription. A subscription-time policy snapshot
            // cannot decide whether the native enclosing merge will resume a later failure.
            StepVerifier.create(query.start(Flux.just(
                            comparisonRow(new FailedComparison(failure)),
                            comparisonRow(new FailedComparison(failure)),
                            comparisonRow(3), comparisonRow(4))
                            .hide().doOnNext(ignore -> {
                                if (consumed.incrementAndGet() == 2) {
                                    Hooks.onNextError((error, value) -> {
                                        Assertions.assertSame(failure, error);
                                        Assertions.assertNull(value);
                                        continued.incrementAndGet();
                                        return null;
                                    });
                                }
                            })), 0)
                    .thenRequest(1)
                    .expectNext(java.util.Collections.singletonMap("highest", 4))
                    .verifyComplete();
            Assertions.assertEquals(1, continued.get());
            Assertions.assertEquals(4, consumed.get());
        } finally {
            Hooks.resetOnNextError();
        }
    }

    private static Map<String, Object> comparisonRow(Object score) {
        Map<String, Object> row = new HashMap<>();
        row.put("type", "a");
        row.put("score", score);
        return row;
    }

    public static class ComparisonRow {
        private final Object score;
        public ComparisonRow(Object score) { this.score = score; }
        public Object getScore() { return score; }
        public String getType() { return "a"; }
    }

    public static class FailedComparison implements Comparable<FailedComparison> {
        private final RuntimeException failure;
        public FailedComparison(RuntimeException failure) { this.failure = failure; }
        @Override public int compareTo(FailedComparison other) { throw failure; }
    }

    @Test
    void binaryCalculationsKeepPublisherSignalsAndRecoveryValues() {
        Assertions.assertAll(Arrays.asList("value + 2", "2 + value", "value * 2", "value / 2", "pow(value + 2, 2)")
                                   .stream().map(argument -> () -> {
            assertBinaryEquivalent("select id," + argument + " calculated from test");
            assertBinaryEquivalent("select " + argument + " calculated,id from test");
        }));
    }

    @Test
    void binaryCalculationsKeepAggregateAndFilterErrorBoundaries() {
        assertBinaryEquivalent("select sum(value + 2) total,avg(value + 2) mean,count(*) rows from test");
        assertBinaryEquivalent("select id from test where value + 2 > 0");
        assertBinaryEquivalent("select sum(value + 2) total,avg(value + 2) mean,count(*) rows from test "
                                       + "group by id%2 order by rows,total");
        assertBinaryEquivalent("select sum(value + 2) total,avg(value + 2) mean,count(*) rows from test "
                                       + "group by _window(2) order by rows,total");
    }

    @Test
    void scalarFunctionFactoriesKeepPublisherSignalsAndRecoveryValues() {
        Assertions.assertAll(Arrays.asList("round(value, 2)", "pow(value, 2)", "power(2, value)",
                                           "substring('abcdef', value, 2)",
                                           "date_add('2024-01-01', value, 'day')",
                                           "repeat('x', value)", "regexp_extract('abc', '(.)', value)")
                                   .stream().map(function -> () -> {
            assertBinaryEquivalent("select id," + function + " calculated from test");
            assertBinaryEquivalent("select " + function + " calculated,id from test");
        }));
    }

    @Test
    void scalarFunctionFactoriesKeepAggregateAndFilterErrorBoundaries() {
        Assertions.assertAll(Arrays.asList("round(value, 2)", "pow(value, 2)").stream().map(function -> () -> {
            assertBinaryEquivalent("select sum(" + function + ") total,avg(" + function
                                           + ") mean,count(*) rows from test");
            assertBinaryEquivalent("select count(*) rows,avg(" + function + ") mean,sum("
                                           + function + ") total from test");
            assertBinaryEquivalent("select id from test where " + function + " > 0");
            assertBinaryEquivalent("select sum(" + function + ") total,avg(" + function
                                           + ") mean,count(*) rows from test group by id%2 order by rows,total");
            assertBinaryEquivalent("select sum(" + function + ") total,avg(" + function
                                           + ") mean,count(*) rows from test group by _window(2) order by rows,total");
        }));
    }

    @Test
    void scalarFunctionErrorConsumerFailureKeepsContextDemandAndOnceOnlyCleanup() {
        for (String function : Arrays.asList("round(value, 2)", "pow(value, 2)",
                                             "date_add('2024-01-01', value, 'day')")) {
            for (boolean optimized : Arrays.asList(false, true)) {
                AtomicInteger errors = new AtomicInteger();
                AtomicInteger cancellations = new AtomicInteger();
                IllegalStateException failure = new IllegalStateException("function error consumer failed");
                StepVerifier.create(new DefaultReactorQL(metadata(
                        "select id," + function + " calculated from test", optimized))
                        .start(Flux.deferContextual(view -> {
                            Assertions.assertEquals("visible", view.get("marker"));
                            return Flux.just(row(1, 4), row(2, "not a number"), row(3, 9));
                        }).concatWith(Flux.never()).doFinally(signal -> {
                            if (signal == reactor.core.publisher.SignalType.CANCEL) {
                                cancellations.incrementAndGet();
                            }
                        }))
                        .onErrorContinue(TypeCastException.class, (error, value) -> {
                            Assertions.assertNull(value);
                            errors.incrementAndGet();
                            throw failure;
                        }).contextWrite(context -> context.put("marker", "visible")), 0)
                            .thenRequest(1)
                            .expectNextCount(1)
                            .thenRequest(1)
                            .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                            .verify();
                Assertions.assertEquals(1, errors.get(), function);
                Assertions.assertEquals(1, cancellations.get(), function);
            }
        }
    }

    @Test
    void binaryCalculatorConsumerFailureKeepsContextAndOnceOnlyCancellation() {
        for (boolean optimized : Arrays.asList(false, true)) {
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger cancellations = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("binary error consumer failed");
            StepVerifier.create(new DefaultReactorQL(metadata(
                    "select id,value + 2 calculated from test", optimized))
                    .start(Flux.deferContextual(view -> {
                        Assertions.assertEquals("visible", view.get("marker"));
                        return Flux.just(row(1, 4), row(2, "not a number"), row(3, 9));
                    }).concatWith(Flux.never()).doFinally(signal -> {
                        // Native composition may call cancel repeatedly; subscription cleanup is once-only.
                        if (signal == reactor.core.publisher.SignalType.CANCEL) {
                            cancellations.incrementAndGet();
                        }
                    }))
                    .onErrorContinue(TypeCastException.class, (error, value) -> {
                        Assertions.assertNull(value);
                        errors.incrementAndGet();
                        throw failure;
                    }).contextWrite(context -> context.put("marker", "visible")))
                        .expectNextCount(1)
                        .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                        .verify();
            Assertions.assertEquals(1, errors.get());
            Assertions.assertEquals(1, cancellations.get());
        }
    }

    private static void assertBinaryEquivalent(String sql) {
        List<Object> oldContinued = new java.util.ArrayList<>();
        List<Object> newContinued = new java.util.ArrayList<>();
        List<Object> expected = binarySignals(sql, false, oldContinued);
        List<Object> actual = binarySignals(sql, true, newContinued);
        Assertions.assertTrue(!oldContinued.isEmpty() || expected.stream().anyMatch(signal ->
                signal instanceof List && "error".equals(((List<?>) signal).get(0))),
                              "Fixture must reach an error boundary: " + sql);
        Assertions.assertEquals(expected, actual, sql + " signals");
        Assertions.assertEquals(oldContinued, newContinued, sql + " continuation values");
    }

    private static List<Object> binarySignals(String sql, boolean optimized, List<Object> continued) {
        return new DefaultReactorQL(metadata(sql, optimized))
                .start(Flux.just(row(1, 4), row(2, "not a number"), row(3, 9), row(4, null)))
                .onErrorContinue((error, value) -> continued.add(value))
                .materialize()
                .<Object>map(signal -> signal.isOnNext() ? signal.get()
                        : signal.isOnError() ? Arrays.asList("error", signal.getThrowable().getClass().getName(),
                                                            signal.getThrowable().getMessage())
                        : java.util.Collections.singletonList("complete"))
                .collectList().block();
    }

    @Test
    void rawThisAggregatesKeepIndependentValueErrorScope() {
        String sql = "select sum(this) total,avg(this) mean,count(this) rows from test";
        List<Map<String, Object>> expected = null;
        for (int mode = 0; mode < 4; mode++) {
            boolean optimized = (mode & 1) != 0;
            Flux<?> source = Flux.just(1, "not a number", 3);
            if (mode >= 2) {
                ReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
                source = source.map(value -> ReactorQLRecord.newRecord("upstream", value, context));
            }
            AtomicInteger errors = new AtomicInteger();
            List<Map<String, Object>> result = new DefaultReactorQL(metadata(sql, optimized))
                    .start(source)
                    .onErrorContinue((error, value) -> {
                        Assertions.assertEquals("not a number", value);
                        errors.incrementAndGet();
                    }).collectList().block();
            Assertions.assertEquals(2, errors.get());
            if (expected == null) {
                expected = result;
                Assertions.assertEquals(4D, result.get(0).get("total"));
                Assertions.assertEquals(2D, result.get(0).get("mean"));
                Assertions.assertEquals(3L, result.get(0).get("rows"));
            } else {
                Assertions.assertEquals(expected, result);
            }
        }
    }

    @Test
    void rawThisAggregateErrorConsumerFailureTerminatesOnce() {
        for (boolean optimized : Arrays.asList(false, true)) {
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger cancellations = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("raw error consumer failed");
            StepVerifier.create(new DefaultReactorQL(metadata(
                    "select sum(this) total,count(this) rows from test", optimized))
                    .start(Flux.just(1, "not a number", 3).concatWith(Flux.never())
                               .doOnCancel(cancellations::incrementAndGet))
                    .onErrorContinue(TypeCastException.class, (error, value) -> {
                        Assertions.assertEquals("not a number", value);
                        errors.incrementAndGet();
                        throw failure;
                    }))
                        .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                        .verify();
            Assertions.assertEquals(1, errors.get());
            Assertions.assertEquals(1, cancellations.get());
        }
    }

    @Test
    void numericFunctionsKeepValueErrorScope() {
        Assertions.assertAll(Arrays.asList("abs", "floor", "ceil", "sqrt", "bit_not", "math.log")
                                   .stream().map(function -> () -> assertEquivalent(
                                           "select id," + function + "(value) calculated from test")));
    }

    @Test
    void nestedSignedArgumentsKeepValueErrorScope() {
        Assertions.assertAll(Arrays.asList("-", "+", "~").stream().map(sign -> () -> {
            assertEquivalent("select id," + sign + "value calculated from test");
            assertEquivalent("select id,abs(" + sign + "value) calculated from test");
            assertEquivalent("select abs(" + sign + "value) calculated,id from test");
            assertEquivalent("select sum(" + sign + "value) total,avg(" + sign
                                     + "value) mean,count(*) rows from test");
        }));
    }

    @Test
    void signedNumericLiteralsArePreparedWithoutChangingValueTypes() {
        String sql = "select -7 negative_integer,+7 positive_integer,-1.5 negative_decimal,"
                + "+1.5 positive_decimal,~7 complemented_integer,-0.0 negative_zero from test";
        for (boolean optimized : Arrays.asList(false, true)) {
            DefaultReactorQLMetadata metadata = metadata(sql, optimized);
            metadata.getSql().getSelectItems().forEach(item -> {
                Expression expression = ((net.sf.jsqlparser.statement.select.SelectExpressionItem) item)
                        .getExpression();
                Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expression, metadata);
                Assertions.assertTrue(mapper instanceof ScalarValueMapper);
                Assertions.assertTrue(((ScalarValueMapper) mapper).isConstant());
            });
            Map<String, Object> result = new DefaultReactorQL(metadata).start(Flux.just(row(1, 4))).single().block();
            Assertions.assertEquals(-7L, result.get("negative_integer"));
            Assertions.assertEquals(7L, result.get("positive_integer"));
            Assertions.assertEquals(-1.5D, result.get("negative_decimal"));
            Assertions.assertEquals(1.5D, result.get("positive_decimal"));
            Assertions.assertEquals(-8L, result.get("complemented_integer"));
            Assertions.assertEquals(Double.doubleToRawLongBits(-0.0D),
                                    Double.doubleToRawLongBits((Double) result.get("negative_zero")));
        }
    }

    @Test
    void invalidSignedLiteralKeepsItsSubscriptionErrorScope() {
        assertEquivalent("select id,abs(-'not a number') calculated from test");
        assertEquivalent("select abs(-'not a number') calculated,id from test");
    }

    @Test
    void signedConstantsInFiltersRetainDemandAndValues() {
        StepVerifier.create(ReactorQL.builder()
                                     .sql("select this value from test where this > -3 and this < +2")
                                     .build()
                                     .start(Flux.just(-4, -3, -2, -1, 0, 1, 2, 3)), 0)
                    .thenRequest(1)
                    .expectNext(java.util.Collections.singletonMap("value", -2))
                    .thenRequest(3)
                    .expectNext(java.util.Collections.singletonMap("value", -1),
                                java.util.Collections.singletonMap("value", 0),
                                java.util.Collections.singletonMap("value", 1))
                    // Request past the accepted rows so the finite source can inspect its filtered tail.
                    .thenRequest(1)
                    .expectComplete()
                    .verify(java.time.Duration.ofSeconds(5));
    }

    @Test
    void castConversionErrorsKeepValueScope() {
        Assertions.assertAll(Arrays.asList("long", "double", "decimal").stream().map(type -> () -> {
            assertEquivalent("select id,cast(value as " + type + ") calculated from test");
            assertEquivalent("select cast(value as " + type + ") calculated,id from test");
            assertEquivalent("select id,abs(cast(value as " + type + ")) calculated from test");
        }));
    }

    @Test
    void castAggregateErrorsKeepIndependentColumns() {
        Assertions.assertAll(Arrays.asList("long", "double", "decimal").stream().map(type -> () -> {
            String argument = "cast(value as " + type + ")";
            assertEquivalent("select sum(" + argument + ") total,avg(" + argument
                                     + ") mean,count(*) rows from test");
            assertEquivalent("select count(*) rows,avg(" + argument + ") mean,sum("
                                     + argument + ") total from test");
            for (String group : Arrays.asList("_window(2)", "id%2", "id%2,_window(2)")) {
                assertEquivalent("select sum(" + argument + ") total,avg(" + argument
                                         + ") mean,count(*) rows from test group by " + group + " order by rows,total");
            }
        }));
    }

    @Test
    void failingCastErrorConsumerTerminatesAndCancelsSource() {
        for (boolean optimized : Arrays.asList(false, true)) {
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger cancellations = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("cast error consumer failed");
            StepVerifier.create(new DefaultReactorQL(metadata("select id,cast(value as long) calculated from test", optimized))
                    .start(Flux.just(row(1, 4), row(2, "not a number"), row(3, 9))
                               .concatWith(Flux.never())
                               .doOnCancel(cancellations::incrementAndGet))
                    .onErrorContinue(TypeCastException.class, (error, value) -> {
                        Assertions.assertEquals("not a number", value);
                        errors.incrementAndGet();
                        throw failure;
                    }))
                        .expectNext(castRow(1, 4L))
                        .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                        .verify(java.time.Duration.ofSeconds(5));
            Assertions.assertEquals(1, errors.get());
            Assertions.assertEquals(1, cancellations.get());
        }
    }

    @Test
    void aggregateConversionErrorsKeepIndependentColumns() {
        assertEquivalent("select sum(value) total,avg(value) mean,count(*) rows,count(value) values from test");
        assertEquivalent("select count(*) rows,count(value) values,avg(value) mean,sum(value) total from test");
    }

    @Test
    void aggregateExpressionErrorsKeepIndependentColumns() {
        assertEquivalent("select sum(abs(value)) total,avg(abs(value)) mean,count(*) rows from test");
        assertEquivalent("select count(*) rows,avg(abs(value)) mean,sum(abs(value)) total from test");
    }

    @Test
    void groupedAndWindowedConversionErrorsKeepTheirCounts() {
        for (String group : Arrays.asList("_window(2)", "id%2", "id%2,_window(2)")) {
            // Group completion order is not a SQL ordering contract; compare a deterministic query.
            assertEquivalent("select sum(value) total,avg(value) mean,count(*) rows from test group by "
                                     + group + " order by rows,total");
            assertEquivalent("select count(*) rows,avg(value) mean,sum(value) total from test group by "
                                     + group + " order by rows,total");
        }
    }

    @Test
    void failingScopedErrorConsumerTerminatesOnceAndCancelsSource() {
        String sql = "select sum(value) total,count(*) rows from test";
        for (boolean optimized : Arrays.asList(false, true)) {
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger cancellations = new AtomicInteger();
            IllegalStateException handlerFailure = new IllegalStateException("error consumer failed");
            StepVerifier.create(new DefaultReactorQL(metadata(sql, optimized))
                    .start(Flux.just(row(1, 4), row(2, "not a number"), row(3, 9))
                               .concatWith(Flux.never())
                               .doOnCancel(cancellations::incrementAndGet))
                    .onErrorContinue(TypeCastException.class, (error, value) -> {
                        errors.incrementAndGet();
                        throw handlerFailure;
                    }))
                        .expectErrorSatisfies(error -> Assertions.assertSame(handlerFailure, error))
                        .verify();
            Assertions.assertEquals(1, errors.get(), "optimized=" + optimized);
            Assertions.assertEquals(1, cancellations.get());
        }
    }

    @Test
    void reductionFailureIsNotRecoveredAsAMappingFailure() {
        IllegalStateException failure = new IllegalStateException("numeric reduction failed");
        Number value = new Number() {
            @Override
            public int intValue() { return 1; }
            @Override
            public long longValue() { return 1; }
            @Override
            public float floatValue() { return 1; }
            @Override
            public double doubleValue() { throw failure; }
        };
        for (boolean optimized : Arrays.asList(false, true)) {
            AtomicInteger continued = new AtomicInteger();
            AtomicInteger cancelled = new AtomicInteger();
            StepVerifier.create(new DefaultReactorQL(metadata("select sum(value) total from test", optimized))
                    .start(Flux.just(row(1, value)).concatWith(Flux.never())
                               .doOnCancel(cancelled::incrementAndGet))
                    .onErrorContinue((error, row) -> continued.incrementAndGet()))
                        .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                        .verify();
            Assertions.assertEquals(0, continued.get());
            Assertions.assertEquals(1, cancelled.get());
        }
    }

    private static void assertEquivalent(String sql) {
        AtomicInteger oldErrors = new AtomicInteger();
        AtomicInteger newErrors = new AtomicInteger();
        List<Map<String, Object>> expected = run(sql, false, oldErrors);
        List<Map<String, Object>> actual = run(sql, true, newErrors);
        Assertions.assertTrue(oldErrors.get() > 0, "Fixture must reach the error boundary: " + sql);
        Assertions.assertEquals(expected, actual, sql);
        Assertions.assertEquals(oldErrors.get(), newErrors.get(), sql);
    }

    private static List<Map<String, Object>> run(String sql, boolean optimized, AtomicInteger errors) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = new DefaultReactorQL(metadata(sql, optimized))
                .start(Flux.just(row(1, 4), row(2, "not a number"), row(3, 9), row(4, null))
                           .doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                .onErrorContinue((error, value) -> {
                    Assertions.assertEquals("not a number", value);
                    errors.incrementAndGet();
                })
                .collectList().block();
        Assertions.assertEquals(1, subscriptions.get(), sql);
        return result;
    }

    private static DefaultReactorQLMetadata metadata(String sql, boolean optimized) {
        DefaultReactorQLMetadata metadata = optimized ? new DefaultReactorQLMetadata(sql) : new DefaultReactorQLMetadata(sql) {
            @Override
            public boolean supportsScalarFastPath() {
                return false;
            }
        };
        if (!optimized) {
            metadata.setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false);
            // A metadata capability does not remove mapper markers. Keep the real property mapper,
            // but expose it as an ordinary Function so consumers select their retained Publisher path.
            metadata.addFeature(new ValueMapFeature() {
                @Override
                public String getId() {
                    return FeatureId.ValueMap.property.getId();
                }

                @Override
                public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                           ReactorQLMetadata owner) {
                    Function<ReactorQLRecord, Publisher<?>> delegate = new PropertyMapFeature()
                            .createMapper(expression, owner);
                    return record -> delegate.apply(record);
                }
            });
        }
        return metadata;
    }

    private static Map<String, Object> row(int id, Object value) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("value", value);
        return row;
    }

    private static Map<String, Object> castRow(int id, long value) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("calculated", value);
        return row;
    }
}
