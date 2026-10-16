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

import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.filter.IfValueMapFeature;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.context.Context;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Native conditional boundaries own parameter construction, branch recovery and fallback. */
class ConditionalNativeCompatibilityTest {

    private static final List<String> EXPRESSIONS = Arrays.asList(
            "coalesce(good,bad)", "coalesce(bad,good)",
            "case when score > 0 then bad else good end",
            "case when score > 0 then good else bad end",
            "case when bad > 0 then good else good end",
            "case when score > 0 then bad end", "if(score > 0,bad,good)",
            "if(bad > 0,good,good)", "if(score > 0,bad)");

    @Test
    void conditionalMappersDoNotMoveNativeErrorsToTheRowStage() {
        for (String expression : EXPRESSIONS) {
            DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                    "select " + expression + " result from events");
            Expression value = ((SelectExpressionItem) metadata.getSql().getSelectItems().get(0)).getExpression();
            Assertions.assertFalse(ValueMapFeature.createMapperNow(value, metadata) instanceof ScalarValueMapper,
                    expression);
        }
    }

    @Test
    void propertyFailuresKeepProjectionValuesConstructionAndContinuationData() {
        for (String expression : EXPRESSIONS) {
            for (String columns : Arrays.asList(expression + " result", "id," + expression + " result",
                    expression + " result,id")) {
                compare("select " + columns + " from events");
            }
        }
    }

    @Test
    void propertyFailuresKeepIndependentAggregateAndHierarchicalGroupResults() {
        for (String expression : EXPRESSIONS) {
            for (String group : Arrays.asList("", " group by type", " group by _window(2),type",
                    " group by type,_window(2)", " group by _window(2),type having total > 0")) {
                compare("select sum(" + expression + ") sum,count(1) total from events" + group);
            }
        }
    }

    @Test
    void selectedCaseBranchFailureRecoversFallbackWithoutLosingIndependentCount() {
        RuntimeException failure = new IllegalStateException("property read failed");
        FailingRow bad = new FailingRow(failure);
        Outcome result = run("select sum(case when score > 0 then bad else good end) sum,"
                + "count(1) total from events", false, true, bad, goodRow(), failure);
        Assertions.assertEquals(Arrays.asList(row("sum", 6D, "total", 2L)), result.values);
        Assertions.assertEquals(Arrays.asList("entry"), result.recovered);
    }

    @Test
    void coalescePreservesMapperConstructionEvenWhenTheFirstValueIsNonempty() {
        RuntimeException failure = new IllegalStateException("property read failed");
        FailingRow bad = new FailingRow(failure);
        Outcome result = run("select sum(coalesce(good,bad)) sum,count(1) total from events",
                false, true, bad, goodRow(), failure);
        Assertions.assertEquals(Arrays.asList(row("sum", 3D, "total", 2L)), result.values);
        Assertions.assertEquals(Arrays.asList("record/bad"), result.recovered);
    }

    @Test
    void unselectedIfBranchRemainsUninvoked() {
        for (boolean nativeChain : Arrays.asList(false, true)) {
            RuntimeException failure = new IllegalStateException("unselected property read failed");
            FailingRow bad = new FailingRow(failure);
            Outcome result = run("select if(score > 0,good,bad) result from events",
                    nativeChain, true, bad, goodRow(), failure);
            Assertions.assertEquals(Arrays.asList(row("result", 2), row("result", 3)), result.values);
            Assertions.assertTrue(result.recovered.isEmpty());
            Assertions.assertTrue(result.hooked.isEmpty());
            Assertions.assertEquals(0, bad.failedReads.get());
        }
    }

    @Test
    void caseKeepsNativePublisherCardinalityForAggregateAndFirstValueConsumers() {
        String expression = "case when score > 0 then good when score > 0 then bad else 0 end";
        for (boolean nativeChain : Arrays.asList(false, true)) {
            DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                    "select " + expression + " result from events");
            Expression value = ((SelectExpressionItem) metadata.getSql().getSelectItems().get(0)).getExpression();
            Function<ReactorQLRecord, Publisher<?>> mapper = nativeChain
                    ? new NativeCase().createMapper(value, metadata)
                    : ValueMapFeature.createMapperNow(value, metadata);
            ReactorQLRecord record = ReactorQLRecord.newRecord("events", goodRow(),
                    ReactorQLContext.ofDatasource(table -> Flux.empty()));
            // The retained feature can emit multiple matching branches. Aggregate consumers
            // consume its full Publisher; projection's Mono.from deliberately takes the first.
            StepVerifier.create(Flux.<Object>from(mapper.apply(record)), 0)
                    .thenRequest(1).expectNext(3).thenRequest(1).expectNext(4).verifyComplete();
            StepVerifier.create(builder("select " + expression + " result from events", nativeChain)
                    .build().start(Flux.just(goodRow())))
                    .expectNext(row("result", 3)).verifyComplete();
            StepVerifier.create(builder("select sum(" + expression + ") sum,count(1) total from events", nativeChain)
                    .build().start(Flux.just(goodRow())))
                    .expectNext(row("sum", 7D, "total", 1L)).verifyComplete();
        }
    }

    @Test
    void contextualParametersAndCancellationKeepNativeDemandLifecycle() {
        for (String expression : Arrays.asList("coalesce(empty,async)",
                "case when score > 0 then async else empty end", "if(score > 0,async,empty)")) {
            for (boolean nativeChain : Arrays.asList(false, true)) {
                TestPublisher<Map<String, Object>> source = TestPublisher.create();
                TestPublisher<Object> parameter = TestPublisher.create();
                AtomicInteger sourceCancelled = new AtomicInteger();
                AtomicInteger parameterCancelled = new AtomicInteger();
                AtomicInteger contextualSubscriptions = new AtomicInteger();
                ValueMapFeature property = new ValueMapFeature() {
                    @Override
                    public String getId() {
                        return FeatureId.ValueMap.property.getId();
                    }

                    @Override
                    public Function<ReactorQLRecord, Publisher<?>> createMapper(
                            Expression value, ReactorQLMetadata metadata) {
                        String name = ((Column) value).getColumnName();
                        if (!"async".equals(name) && !"empty".equals(name)) {
                            return new PropertyMapFeature().createMapper(value, metadata);
                        }
                        return record -> Mono.deferContextual(context -> {
                            Assertions.assertEquals("kept", context.get("conditional-test"));
                            contextualSubscriptions.incrementAndGet();
                            return "empty".equals(name) ? Mono.empty()
                                    : parameter.flux().next().doOnCancel(parameterCancelled::incrementAndGet);
                        });
                    }
                };
                ReactorQL query = builder("select " + expression + " result from events", nativeChain)
                        .feature(property).build();
                // Native flatMap subscribes parameters even while output demand is zero;
                // downstream cancellation must still reach both active sources once.
                StepVerifier.create(query.start(source.flux().doOnCancel(sourceCancelled::incrementAndGet))
                        .contextWrite(Context.of("conditional-test", "kept")), 0)
                        .then(() -> source.next(goodRow()))
                        .then(() -> parameter.assertSubscribers(1))
                        .thenCancel().verify();
                source.assertCancelled();
                parameter.assertCancelled();
                Assertions.assertEquals(1, sourceCancelled.get(), expression);
                Assertions.assertEquals(1, parameterCancelled.get(), expression);
                Assertions.assertEquals(expression.startsWith("coalesce") ? 2 : 1,
                        contextualSubscriptions.get(), expression);
            }
        }
    }

    @Test
    void conditionalProjectionKeepsSourceErrorIdentityAndDoesNotContinueIt() {
        RuntimeException failure = new IllegalStateException("source failed");
        for (String expression : Arrays.asList("coalesce(good,missing)",
                "case when score > 0 then good else missing end", "if(score > 0,good,missing)")) {
            for (boolean nativeChain : Arrays.asList(false, true)) {
                AtomicInteger recovered = new AtomicInteger();
                TestPublisher<Map<String, Object>> source = TestPublisher.create();
                StepVerifier.create(builder("select " + expression + " result from events", nativeChain)
                        .build().start(source.flux()).onErrorContinue((error, value) -> recovered.incrementAndGet()), 0)
                        .thenRequest(1).then(() -> source.next(goodRow())).expectNext(row("result", 3))
                        .then(() -> source.error(failure))
                        .expectErrorMatches(error -> error == failure).verify();
                Assertions.assertEquals(0, recovered.get(), expression);
            }
        }
    }

    private void compare(String sql) {
        RuntimeException failure = new IllegalStateException("property read failed");
        FailingRow bad = new FailingRow(failure);
        Map<String, Object> good = goodRow();
        for (boolean continuation : Arrays.asList(false, true)) {
            Outcome nativeRun = run(sql, true, continuation, bad, good, failure);
            Outcome currentRun = run(sql, false, continuation, bad, good, failure);
            String scenario = sql + "/continue=" + continuation;
            Assertions.assertEquals(nativeRun.values, currentRun.values, scenario);
            Assertions.assertEquals(nativeRun.recovered, currentRun.recovered, scenario);
            Assertions.assertEquals(nativeRun.hooked, currentRun.hooked, scenario);
            Assertions.assertEquals(nativeRun.consumed, currentRun.consumed, scenario);
            Assertions.assertEquals(nativeRun.cancelled, currentRun.cancelled, scenario);
        }
    }

    private Outcome run(String sql, boolean nativeChain, boolean continuation,
                        FailingRow bad, Map<String, Object> good, RuntimeException failure) {
        Outcome outcome = new Outcome();
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        Hooks.onOperatorError("conditional-native-boundary", (error, value) -> {
            Assertions.assertSame(failure, error, sql);
            outcome.hooked.add(kind(value, bad));
            return error;
        });
        try {
            Flux<Map<String, Object>> result = builder(sql, nativeChain).build()
                    .start(Flux.just(bad, good).doOnNext(value -> consumed.incrementAndGet())
                            .doOnCancel(cancelled::incrementAndGet));
            if (continuation) {
                result = result.onErrorContinue((error, value) -> {
                    Assertions.assertSame(failure, error, sql);
                    outcome.recovered.add(kind(value, bad));
                });
            }
            StepVerifier.Step<Map<String, Object>> verifier = StepVerifier.create(result, 0)
                    .thenRequest(Long.MAX_VALUE)
                    .thenConsumeWhile(value -> {
                        outcome.values.add(new LinkedHashMap<>(value));
                        return true;
                    });
            if (continuation) {
                verifier.verifyComplete();
            } else {
                verifier.expectErrorMatches(error -> error == failure).verify();
            }
            outcome.consumed = consumed.get();
            outcome.cancelled = cancelled.get();
            return outcome;
        } finally {
            Hooks.resetOnOperatorError("conditional-native-boundary");
        }
    }

    private static ReactorQL.Builder builder(String sql, boolean nativeChain) {
        ReactorQL.Builder builder = ReactorQL.builder().sql(sql);
        return nativeChain ? builder.feature(new NativeCoalesce()).feature(new NativeCase())
                .feature(new NativeIf()) : builder;
    }

    private static String kind(Object value, FailingRow bad) {
        if (value == null) return "null";
        if (value instanceof ReactorQLRecord) {
            return ((ReactorQLRecord) value).getRecord() == bad ? "record/bad" : "record/other";
        }
        if (value instanceof Map.Entry) return "entry";
        return value.getClass().getName();
    }

    private static Map<String, Object> goodRow() {
        return row("id", 2, "good", 3, "bad", 4, "score", 1, "type", "a");
    }

    private static Map<String, Object> row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return row;
    }

    private static final class FailingRow extends AbstractMap<String, Object> {
        private final RuntimeException failure;
        private final AtomicInteger failedReads = new AtomicInteger();
        private final Map<String, Object> values = row("id", 1, "good", 2, "bad", 9, "score", 1, "type", "a");

        private FailingRow(RuntimeException failure) { this.failure = failure; }

        @Override
        public Object get(Object key) {
            if ("bad".equals(key)) {
                failedReads.incrementAndGet();
                throw failure;
            }
            return values.get(key);
        }

        @Override
        public Set<Entry<String, Object>> entrySet() { return values.entrySet(); }
    }

    private static final class Outcome {
        private final List<Map<String, Object>> values = new ArrayList<>();
        private final List<String> recovered = new ArrayList<>();
        private final List<String> hooked = new ArrayList<>();
        private int consumed;
        private int cancelled;
    }

    /** Independent retained native composition; never delegates to the candidate factory. */
    private static final class NativeCoalesce extends CoalesceMapFeature {
        @Override
        public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
            List<Function<ReactorQLRecord, Publisher<?>>> mappers = ExpressionUtils
                    .getFunctionParameter((net.sf.jsqlparser.expression.Function) expression).stream()
                    .map(parameter -> ValueMapFeature.createMapperNow(parameter, metadata)).collect(Collectors.toList());
            return record -> {
                Flux<Object> result = null;
                for (Function<ReactorQLRecord, Publisher<?>> mapper : mappers) {
                    Flux<Object> next = Flux.from(mapper.apply(record));
                    result = result == null ? next : result.switchIfEmpty(next);
                }
                return result == null ? Flux.empty() : result;
            };
        }
    }

    /** Native branch entries remain continuation data, including recovery into ELSE. */
    private static final class NativeCase extends CaseMapFeature {
        @Override
        public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
            CaseExpression conditional = (CaseExpression) expression;
            Function<ReactorQLRecord, Publisher<?>> switchMapper = conditional.getSwitchExpression() == null
                    ? (ScalarValueMapper) ReactorQLRecord::getRecord
                    : ValueMapFeature.createMapperNow(conditional.getSwitchExpression(), metadata);
            Map<BiFunction<ReactorQLRecord, Object, Mono<Boolean>>, Function<ReactorQLRecord, Publisher<?>>> branches
                    = new LinkedHashMap<>();
            for (WhenClause branch : conditional.getWhenClauses()) {
                branches.put(FilterFeature.createPredicateNow(branch.getWhenExpression(), metadata),
                        createThen(branch.getThenExpression(), metadata));
            }
            Function<ReactorQLRecord, Publisher<?>> otherwise = createThen(conditional.getElseExpression(), metadata);
            return record -> {
                Mono<?> value = Mono.from(switchMapper.apply(record));
                return Flux.fromIterable(branches.entrySet())
                        .filterWhen(branch -> value.flatMap(input -> branch.getKey().apply(record, input)))
                        .<Object>flatMap(branch -> branch.getValue().apply(record))
                        .switchIfEmpty(Flux.from(otherwise.apply(record)));
            };
        }
    }

    /** Native IF keeps branch construction inside the condition's flatMap. */
    private static final class NativeIf extends IfValueMapFeature {
        @Override
        public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
            List<Expression> parameters = ExpressionUtils.getFunctionParameter(
                    (net.sf.jsqlparser.expression.Function) expression);
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate =
                    FilterFeature.createPredicateNow(parameters.get(0), metadata);
            Function<ReactorQLRecord, Publisher<?>> selected = ValueMapFeature.createMapperNow(parameters.get(1), metadata);
            Function<ReactorQLRecord, Publisher<?>> otherwise = parameters.size() == 3
                    ? ValueMapFeature.createMapperNow(parameters.get(2), metadata)
                    : (ScalarValueMapper) record -> null;
            return record -> Mono.from(predicate.apply(record, record)).defaultIfEmpty(false)
                    .flatMap(matched -> Mono.from((matched ? selected : otherwise).apply(record)));
        }
    }
}
