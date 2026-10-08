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
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import reactor.core.Exceptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.time.Duration;
import java.util.function.Function;

@Timeout(10)
class AggregateNativeCompatibilityTest {

    @Test
    void shouldAggregateDefaultMapSourceWithoutPerRowRecord() {
        String sql = "select count(1) total,sum(t.score) sum,avg(t.score) avg,"
                + "min(t.score) min,max(t.score) max from test t";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        Flux<Map<String, Object>> rows = Flux.just(row("a", 1), row("b", 2), row("c", 3));
        Map<String, Object> expected = map("total", 3L, "sum", 6D, "avg", 2D,
                                           "min", 1, "max", 3);
        StepVerifier.create(optimized.start(rows), 0)
                .thenRequest(1)
                .expectNext(expected)
                .verifyComplete();
        StepVerifier.create(legacy.start(rows))
                .expectNext(expected)
                .verifyComplete();
        StepVerifier.create(Flux.zip(optimized.start(rows), optimized.start(rows)))
                .assertNext(pair -> {
                    Assertions.assertEquals(expected, pair.getT1());
                    Assertions.assertEquals(expected, pair.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldFilterRawAndNonMapRowsBeforeGlobalAggregation() {
        String sql = "select count(1) total,sum(t.score) sum,avg(t.score) avg,"
                + "min(t.score) min,max(t.score) max from test t "
                + "where t.score >= 2 and t.score < 4";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        Flux<Object> rows = Flux.just(row("a", 1), new ScoreRow(2), row("b", 3),
                                      new ScoreRow(4), map("other", 1));
        Map<String, Object> expected = map("total", 2L, "sum", 5D, "avg", 2.5D,
                                           "min", 2, "max", 3);
        StepVerifier.create(optimized.start(rows), 0)
                .thenRequest(1)
                .expectNext(expected)
                .verifyComplete();
        StepVerifier.create(legacy.start(rows))
                .expectNext(expected)
                .verifyComplete();
        StepVerifier.create(Flux.zip(optimized.start(rows), optimized.start(rows)))
                .assertNext(pair -> {
                    Assertions.assertEquals(expected, pair.getT1());
                    Assertions.assertEquals(expected, pair.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldKeepFilteredRawAggregateSignalsAndFallbacks() {
        ReactorQL query = ReactorQL.builder()
                .sql("select count(1) total,sum(score) sum from test "
                             + "where score >= 2 or score < 0")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(query.start(Flux.deferContextual(view -> {
                                     Assertions.assertEquals("visible", view.get("marker"));
                                     return Flux.just(row("a", 1), row("b", 2), row("c", 3));
                                 }))
                                 .contextWrite(context -> context.put("marker", "visible")), 0)
                .thenRequest(1)
                .expectNext(map("total", 2L, "sum", 5D))
                .verifyComplete();

        RuntimeException failure = new RuntimeException("filtered source failed");
        StepVerifier.create(query.start(Flux.concat(Flux.just(row("a", 2)), Flux.error(failure))))
                .expectErrorMatches(error -> error == failure)
                .verify();
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger subscriptions = new AtomicInteger();
        StepVerifier.create(query.start(Flux.concat(Flux.just(row("a", 2)), Flux.never())
                                             .doOnSubscribe(ignore -> subscriptions.incrementAndGet())
                                             .doOnCancel(() -> cancelled.set(true))), 0)
                .thenRequest(1)
                .thenCancel()
                .verify();
        Assertions.assertEquals(subscriptions.get() != 0, cancelled.get());

        ReactorQL nested = ReactorQL.builder()
                .sql("select sum(score) total from test where payload.score > 1")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) nested).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        org.jetlinks.reactor.ql.supports.DefaultPropertyFeature custom =
                new org.jetlinks.reactor.ql.supports.DefaultPropertyFeature() {
                    @Override
                    public Optional<Object> getProperty(Object property, Object source) {
                        return "score".equals(property)
                                ? Optional.of(10)
                                : super.getProperty(property, source);
                    }
                };
        ReactorQL customQuery = ReactorQL.builder()
                .feature(custom)
                .sql("select sum(score) total from test where score >= 2")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) customQuery).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(customQuery.start(Flux.just(row("a", 1), row("b", 2))))
                .expectNext(map("total", 20D))
                .verifyComplete();
    }

    public static class ScoreRow {
        private final int score;

        public ScoreRow(int score) {
            this.score = score;
        }

        public int getScore() {
            return score;
        }
    }

    @Test
    void shouldKeepOrdinarySourceAggregationEquivalentAcrossGroupShapes() {
        for (String group : Arrays.asList("", " group by _window(3),t.type,t.region",
                " group by t.type,t.region")) {
            String sql = "select " + (group.isEmpty() ? "" : "type,region,")
                    + "count(t.score) total,sum(t.score) sum,avg(t.score) avg,"
                    + "min(t.score) min,max(t.score) max from test t" + group;
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql).build();
            AtomicInteger subscriptions = new AtomicInteger();
            Flux<Object> rows = Flux.defer(() -> {
                subscriptions.incrementAndGet();
                return Flux.just(new MeasurementRow(1), map("type", "a", "region", "site", "score", 2),
                        new MeasurementRow(null));
            });
            Map<String, Object> expected = map("total", 2L, "sum", 3D, "avg", 1.5D, "min", 1, "max", 2);
            if (!group.isEmpty()) {
                expected.put("type", "a");
                expected.put("region", "site");
            }
            StepVerifier.create(legacy.start(rows)).expectNext(expected).verifyComplete();
            StepVerifier.create(optimized.start(rows), 0).thenRequest(1).expectNext(expected).verifyComplete();
            StepVerifier.create(optimized.start(rows), 0).thenRequest(1).expectNext(expected).verifyComplete();
            Assertions.assertEquals(3, subscriptions.get());
        }
    }

    @Test
    void shouldKeepVirtualResultPropertiesAndGeneratedKeysForOrdinarySources() {
        for (String group : Arrays.asList("", " group by _window(1),type")) {
            String sql = "select count(size) sizes,count(keys) keys,count(empty) empty,"
                    + "count(missing) missing_count from test" + group;
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql).build();
            Flux<MeasurementRow> rows = Flux.just(new MeasurementRow(null));
            Map<String, Object> expected = map("sizes", 1L, "keys", 1L, "empty", 1L, "missing_count", 0L);
            if (!group.isEmpty()) {
                expected.put("type", "a");
            }
            StepVerifier.create(legacy.start(rows)).expectNext(expected).verifyComplete();
            StepVerifier.create(optimized.start(rows), 0).thenRequest(1).expectNext(expected).verifyComplete();
        }
        String sql = "select count(_group_by_key) total from test group by _window(2),type";
        Flux<MeasurementRow> rows = Flux.just(new MeasurementRow(1), new MeasurementRow(2));
        for (boolean optimized : Arrays.asList(false, true)) {
            StepVerifier.create(ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, optimized).sql(sql).build().start(rows))
                    .expectNext(map("type", "a", "total", 2L)).verifyComplete();
        }
    }

    @Test
    void shouldKeepOrdinarySourceNumericConversionErrorScope() {
        for (String group : Arrays.asList("", " group by _window(3),type")) {
            String sql = "select count(score) total,sum(score) sum,avg(score) avg from test" + group;
            for (boolean optimized : Arrays.asList(false, true)) {
                ReactorQL query = ReactorQL.builder()
                        .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, optimized).sql(sql).build();
                AtomicInteger errors = new AtomicInteger();
                Map<String, Object> expected = map("total", 3L, "sum", 4D, "avg", 2D);
                if (!group.isEmpty()) {
                    expected.put("type", "a");
                }
                StepVerifier.create(query.start(Flux.deferContextual(view -> {
                    Assertions.assertEquals("visible", view.get("marker"));
                    return Flux.just(new MeasurementRow(1), new MeasurementRow("not a number"),
                            new MeasurementRow(3));
                })).onErrorContinue((error, value) -> {
                    Assertions.assertEquals("not a number", value);
                    errors.incrementAndGet();
                }).contextWrite(context -> context.put("marker", "visible")), 0)
                        .thenRequest(1).expectNext(expected).verifyComplete();
                Assertions.assertEquals(2, errors.get());
            }
        }
    }

    @Test
    void shouldKeepOrdinarySourceDemandAndCancellation() {
        ReactorQL query = ReactorQL.builder()
                .sql("select count(1) total,sum(score) sum from test "
                        + "where score>=0 group by _window(2),type,region")
                .build();
        AtomicInteger visited = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        Flux<MeasurementRow> source = Flux.concat(Flux.range(0, 1000).hide()
                .map(value -> {
                    visited.incrementAndGet();
                    return new MeasurementRow(value);
                }), Flux.never()).doOnCancel(() -> cancelled.set(true));
        StepVerifier.create(query.start(source), 0)
                .then(() -> Assertions.assertEquals(1000, visited.get()))
                .thenRequest(1).expectNext(map("type", "a", "region", "site", "total", 2L, "sum", 1D))
                .thenCancel().verify();
        Assertions.assertTrue(cancelled.get());
        Assertions.assertEquals(1000, visited.get());
        RuntimeException failure = new RuntimeException("ordinary source failed");
        StepVerifier.create(query.start(Flux.concat(Flux.just(new MeasurementRow(1)), Flux.error(failure))))
                .expectErrorMatches(error -> error == failure).verify();
    }

    /** Ordinary nullable bean properties; no prepared Map or special input marker. */
    public static class MeasurementRow {
        private final Object score;

        public MeasurementRow(Object score) {
            this.score = score;
        }

        public String getType() { return "a"; }
        public String getRegion() { return "site"; }
        public Object getScore() { return score; }
    }

    @Test
    void shouldAggregateRowIndependentExpressionsAcrossInputShapes() {
        String sql = "select count(1) total,sum(1) sum,avg(1) avg,"
                + "min(1) min,max(1) max from test";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        Flux<?> integers = Flux.range(0, 4).hide();
        Flux<?> mixed = Flux.just(1, row("a", 2), "three", row("b", 4));
        Assertions.assertEquals(legacy.start(integers).collectList().block(),
                                optimized.start(integers).collectList().block());
        Assertions.assertEquals(legacy.start(mixed).collectList().block(),
                                optimized.start(mixed).collectList().block());
    }

    @Test
    void shouldAggregateAnyRawRowsOnlyWhenFilterAndAggregateDeclareSupport() {
        ReactorQL optimized = ReactorQL.builder()
                .sql("select count(1) total from test where this >= 1 and this < 3")
                .build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql("select count(1) total from test where this >= 1 and this < 3")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));
        Flux<Object> numbers = Flux.just(0, 1, 2, 3);
        Assertions.assertEquals(legacy.start(numbers).collectList().block(),
                                optimized.start(numbers).collectList().block());
        StepVerifier.create(optimized.start(Flux.just(0, 1, 2, 3)), 0)
                .thenRequest(1)
                .expectNext(map("total", 2L))
                .verifyComplete();

        ReactorQL stringQuery = ReactorQL.builder()
                                           .sql("select count(1) total from test where this >= 'b' and this < 'd'")
                                           .build();
        StepVerifier.create(stringQuery.start(Flux.just("a", "b", "c", "d")))
                .expectNext(map("total", 2L))
                .verifyComplete();

        ReactorQL mapQuery = ReactorQL.builder()
                                        .sql("select count(1) total from test where this >= 2")
                                        .build();
        Assertions.assertEquals(
                ReactorQL.builder().setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                         .sql("select count(1) total from test where this >= 2")
                         .build().start(Flux.just(map("value", 1), map("value", 2))).collectList().block(),
                mapQuery.start(Flux.just(map("value", 1), map("value", 2))).collectList().block());
    }

    @Test
    void shouldKeepThisAggregateAndParameterFallbackSemantics() {
        for (String sql : Arrays.asList(
                "select count(this) total,sum(this) sum,avg(this) avg,min(this) min,max(this) max from test",
                "select count(this) total,sum(this) sum,avg(this) avg,min(this) min,max(this) max from test where this >= 2 or this < 0",
                "select count(distinct this) total,count(unique this) singletons from test")) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql)
                    .build();
            Flux<Integer> rows = Flux.just(1, 2, 3);
            Assertions.assertEquals(legacy.start(rows).collectList().block(),
                                    optimized.start(rows).collectList().block(), sql);
        }
    }

    @Test
    void shouldKeepSourceRecordReadsAndAliasBindingForThisAggregates() {
        String aggregate = "select count(this) total,sum(this) sum,avg(this) avg,"
                + "min(this) min,max(this) max from test t";
        for (String sql : Arrays.asList(aggregate, aggregate + " where this >= 2")) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                                       .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                       .sql(sql).build();
            ReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
            List<ReactorQLRecord> records = Arrays.asList(
                    ReactorQLRecord.newRecord("upstream", 1, context),
                    ReactorQLRecord.newRecord("upstream", 2, context),
                    ReactorQLRecord.newRecord("upstream", 3, context));
            List<Map<String, Object>> expected = legacy.start(Flux.just(1, 2, 3)).collectList().block();
            Assertions.assertEquals(expected, optimized.start(Flux.fromIterable(records)).collectList().block(), sql);
            records.forEach(record -> {
                Assertions.assertEquals("t", record.getName());
                Assertions.assertEquals(record.getRecord(), record.getRecordValue("t"));
                Assertions.assertEquals(record.getRecord(), record.getRecordValue("upstream"));
            });
            // The same query can consume already-bound Records again without treating them as values.
            Assertions.assertEquals(expected, optimized.start(Flux.fromIterable(records)).collectList().block(), sql);
            Assertions.assertEquals(expected,
                                    optimized.start(Flux.just(1, records.get(1), 3)).collectList().block(), sql);
        }
    }

    @Test
    void shouldPreserveScalarInputAggregateContextDemandErrorsAndCancellation() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select count(this) total,sum(this) sum,avg(this) avg,"
                                                + "min(this) min,max(this) max from test")
                                   .build();
        AtomicInteger subscriptions = new AtomicInteger();
        StepVerifier.create(query.start(Flux.deferContextual(view -> {
                            Assertions.assertEquals("visible", view.get("marker"));
                            return Flux.just(1, 2, 3);
                        }).doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                                 .contextWrite(context -> context.put("marker", "visible")), 0)
                    .thenRequest(1)
                    .expectNext(map("total", 3L, "sum", 6D, "avg", 2D, "min", 1, "max", 3))
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());
        RuntimeException failure = new RuntimeException("scalar source failed");
        StepVerifier.create(query.start(Flux.concat(Flux.just(1), Flux.error(failure))))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
        AtomicInteger cancellations = new AtomicInteger();
        AtomicInteger activeSubscriptions = new AtomicInteger();
        StepVerifier.create(query.start(Flux.concat(Flux.just(1), Flux.never())
                                             .doOnSubscribe(ignore -> activeSubscriptions.incrementAndGet())
                                             .doOnCancel(cancellations::incrementAndGet)), 0)
                    .thenRequest(1).thenCancel().verify();
        Assertions.assertEquals(activeSubscriptions.get(), cancellations.get());
        Assertions.assertTrue(activeSubscriptions.get() <= 1);
        ReactorQL legacy = ReactorQL.builder()
                                   .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                   .sql("select count(this) total,sum(this) sum,avg(this) avg,"
                                                + "min(this) min,max(this) max from test")
                                   .build();
        Assertions.assertEquals(legacy.start(Flux.empty()).collectList().block(),
                                query.start(Flux.empty()).collectList().block());
    }

    @Test
    void shouldKeepRecordFallbackForMixedRowsWhenAggregateReadsFields() {
        String sql = "select count(score) total,sum(score) sum,avg(score) avg,"
                + "min(score) min,max(score) max from test";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        Flux<?> mixed = Flux.just(row("a", 1), 42, row("b", 3));
        Assertions.assertEquals(legacy.start(mixed).collectList().block(),
                                optimized.start(mixed).collectList().block());
    }

    @Test
    void shouldPreserveRawAggregateErrorCancellationAndContext() {
        ReactorQL query = ReactorQL.builder()
                .sql("select count(1) total,sum(score) sum from test")
                .build();
        RuntimeException failure = new RuntimeException("source failed");
        StepVerifier.create(query.start(Flux.concat(Flux.just(row("a", 1)), Flux.error(failure))))
                .expectErrorMatches(error -> error == failure)
                .verify();

        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger subscriptions = new AtomicInteger();
        StepVerifier.create(query.start(Flux.concat(Flux.just(row("a", 1)), Flux.never())
                                             .doOnSubscribe(ignore -> subscriptions.incrementAndGet())
                                             .doOnCancel(() -> cancelled.set(true))), 0)
                .thenRequest(1)
                .thenCancel()
                .verify();
        Assertions.assertEquals(subscriptions.get() != 0, cancelled.get());

        StepVerifier.create(query.start(Flux.deferContextual(view -> {
                    Assertions.assertEquals("visible", view.get("marker"));
                    return Flux.just(row("a", 1), row("b", 2));
                })).contextWrite(context -> context.put("marker", "visible")))
                .expectNext(map("total", 2L, "sum", 3D))
                .verifyComplete();
    }

    @Test
    void shouldFallBackFromRawAggregateForCustomPropertyAndNestedColumn() {
        ReactorQL nested = ReactorQL.builder()
                .sql("select sum(payload.score) total from test")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) nested).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        org.jetlinks.reactor.ql.supports.DefaultPropertyFeature custom =
                new org.jetlinks.reactor.ql.supports.DefaultPropertyFeature() {
                    @Override
                    public Optional<Object> getProperty(Object property, Object source) {
                        if ("score".equals(property)) {
                            return Optional.of(10);
                        }
                        return super.getProperty(property, source);
                    }
                };
        ReactorQL query = ReactorQL.builder()
                .feature(custom)
                .sql("select sum(score) total from test")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(query.start(Flux.just(row("a", 1), row("b", 2))))
                .expectNext(map("total", 20D))
                .verifyComplete();
    }

    @Test
    void shouldKeepBoundParameterAggregateOnRecordPath() {
        String sql = "select count(?) total from test";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        ReactorQLContext optimizedContext = ReactorQLContext
                .ofDatasource(ignore -> Flux.range(0, 3))
                .bind(0, 5);
        ReactorQLContext legacyContext = ReactorQLContext
                .ofDatasource(ignore -> Flux.range(0, 3))
                .bind(0, 5);
        Assertions.assertEquals(legacy.start(legacyContext).map(ReactorQLRecord::asMap).collectList().block(),
                                optimized.start(optimizedContext).map(ReactorQLRecord::asMap).collectList().block());
    }

    @Test
    void shouldFuseGlobalScalarAggregatesWithoutRetainingSourceRow() {
        String sql = "select count(1) total,sum(score) sum,avg(score) avg,"
                + "min(score) min,max(score) max from test where score > 0";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();

        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        Flux<Map<String, Object>> rows = Flux.just(row("a", 1), row("b", 2), row("c", 3));
        Map<String, Object> expected = map("total", 3L, "sum", 6D, "avg", 2D,
                                           "min", 1, "max", 3);
        StepVerifier.create(optimized.start(rows), 0)
                .thenRequest(1)
                .expectNext(expected)
                .verifyComplete();
        StepVerifier.create(legacy.start(rows))
                .expectNext(expected)
                .verifyComplete();
        StepVerifier.create(Flux.zip(optimized.start(rows), optimized.start(rows)))
                .assertNext(pair -> {
                    Assertions.assertEquals(expected, pair.getT1());
                    Assertions.assertEquals(expected, pair.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldPreserveGlobalAggregateEmptyAndNullSemantics() {
        for (String sql : Arrays.asList(
                "select count(1) total from test",
                "select sum(score) total from test",
                "select avg(score) total from test",
                "select min(score) total from test",
                "select max(score) total from test",
                "select count(1) total,max(score) max from test")) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql)
                    .build();
            Assertions.assertEquals(legacy.start(Flux.empty()).collectList().block(),
                                    optimized.start(Flux.empty()).collectList().block(), sql);
            Map<String, Object> nullScore = new HashMap<>();
            nullScore.put("score", null);
            Assertions.assertEquals(legacy.start(Flux.just(nullScore)).collectList().block(),
                                    optimized.start(Flux.just(nullScore)).collectList().block(), sql);
        }
    }

    @Test
    void shouldKeepLastRowProjectionOnCompatiblePath() {
        ReactorQL query = ReactorQL.builder()
                .sql("select score,count(1) total from test")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(query.start(Flux.just(row("a", 1), row("b", 2))))
                .expectNext(map("score", 2, "total", 2L))
                .verifyComplete();
    }

    @Test
    void shouldPreserveAllNullWindowAggregateSemantics() {
        for (String sql : Arrays.asList(
                "select type,sum(score) total from test group by _window(2),type",
                "select type,avg(score) total from test group by _window(2),type",
                "select type,min(score) total from test group by _window(2),type",
                "select type,max(score) total from test group by _window(2),type",
                "select type,count(1) total,sum(score) sum from test group by _window(2),type")) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql)
                    .build();
            Map<String, Object> row = new HashMap<>();
            row.put("type", "a");
            row.put("score", null);
            Flux<Map<String, Object>> rows = Flux.just(row, row);
            Assertions.assertEquals(legacy.start(rows).collectList().block(),
                                    optimized.start(rows).collectList().block(), sql);
        }
    }

    @Test
    void shouldFuseCountWindowCompositeGroupAndBuiltInAggregates() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select type,count(1) total,sum(score) sum,avg(score) avg,"
                             + "min(score) min,max(score) max,_group_by_key keys "
                             + "from test group by _window(4),type")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("ASYNC_OR_STATEFUL[projection]"));

        List<Map<String, Object>> result = query
                .start(Flux.just(row("a", 1),
                                 row("b", 2),
                                 row("a", 3),
                                 row("b", 4),
                                 row("a", 5),
                                 row("b", 6)))
                .collectList()
                .block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(4, result.size());
        assertAggregate(result.get(0), "a", 2L, 4D, 2D, 1, 3);
        assertAggregate(result.get(1), "b", 2L, 6D, 3D, 2, 4);
        assertAggregate(result.get(2), "a", 1L, 5D, 5D, 5, 5);
        assertAggregate(result.get(3), "b", 1L, 6D, 6D, 6, 6);
        Assertions.assertEquals(Arrays.asList("a"), result.get(0).get("keys"));
    }

    @Test
    void shouldPreserveMultipleGroupKeysAndAccumulatorsWithReusedLookup() {
        ReactorQL query = ReactorQL.builder()
                .sql("select type,region,count(1) total,sum(score) sum,avg(score) avg "
                             + "from test group by _window(5),type,region")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        Flux<Map<String, Object>> source = Flux.just(
                map("type", "a", "region", "x", "score", 1),
                map("type", "b", "region", "x", "score", 2),
                map("type", "a", "region", "y", "score", 3),
                map("type", "a", "region", "x", "score", 4),
                map("type", "b", "region", "x", "score", 5));
        StepVerifier.create(query.start(source).collectList())
                .assertNext(results -> {
                    Assertions.assertEquals(3, results.size());
                    Assertions.assertEquals(new HashSet<>(Arrays.asList(
                            map("type", "a", "region", "x", "total", 2L,
                                "sum", 5D, "avg", 2.5D),
                            map("type", "b", "region", "x", "total", 2L,
                                "sum", 7D, "avg", 3.5D),
                            map("type", "a", "region", "y", "total", 1L,
                                "sum", 3D, "avg", 3D))), new HashSet<>(results));
                })
                .verifyComplete();
    }

    @Test
    void shouldPreserveNativeGroupKeyTypeAndIdentityForExtensionRecords() {
        ReactorQL query = ReactorQL.builder()
                .sql("select type,count(1) total,_group_by_key keys "
                        + "from test group by _window(1),type")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        // An extension Record may expose key metadata as a collection, array or single value.
        // Native projection exposes the extension's value without normalizing its type or identity.
        for (Function<List<Object>, Object> shape : Arrays.<Function<List<Object>, Object>>asList(
                keys -> keys, List::toArray, keys -> keys.get(0))) {
            DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
            DefaultReactorQLRecord source = new DefaultReactorQLRecord("test", row("a", 1), context) {
                @Override
                public ReactorQLRecord addRecord(String name, Object value) {
                    return super.addRecord(name, GroupFeature.groupByKeyContext.equals(name)
                            ? shape.apply(CastUtils.castArray(value)) : value);
                }
            };
            StepVerifier.create(query.start(Flux.just(source)), 0)
                    .thenRequest(1)
                    .assertNext(result -> {
                        Assertions.assertEquals("a", result.get("type"));
                        Assertions.assertEquals(1L, result.get("total"));
                        Object keys = result.get("keys");
                        Assertions.assertEquals(Arrays.asList("a"), CastUtils.castArray(keys));
                        Object original = source.getRecordValue(GroupFeature.groupByKeyContext);
                        Assertions.assertSame(original, keys);
                        if (original instanceof List) {
                            ((List<Object>) original).set(0, "source-changed");
                            Assertions.assertEquals(Arrays.asList("source-changed"), CastUtils.castArray(keys));
                        } else if (original instanceof Object[]) {
                            ((Object[]) original)[0] = "source-changed";
                            Assertions.assertEquals(Arrays.asList("source-changed"), CastUtils.castArray(keys));
                        } else {
                            source.addRecord(GroupFeature.groupByKeyContext, "source-changed");
                            Assertions.assertEquals("a", keys);
                        }
                    })
                    .verifyComplete();
        }
    }

    @Test
    void shouldPreserveContextResultFallbackBeforeRawReads() {
        for (String sql : Arrays.asList("select count(virtual) total from test",
                "select type,count(virtual) total from test group by _window(1),type",
                "select type from test where virtual=7")) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .feature(new DefaultPropertyFeature())
                    .sql(sql).build();
            Function<Boolean, ReactorQLContext> context = ignored ->
                    new DefaultReactorQLContext(name -> Flux.just(row("a", 1))) {
                        @Override
                        public Map<String, Object> newContainer() {
                            return map("virtual", 7);
                        }
                    };
            List<Map<String, Object>> expected = legacy.start(context.apply(false))
                    .map(ReactorQLRecord::asMap).collectList().block();
            Assertions.assertNotNull(expected);
            Assertions.assertEquals(1, expected.size());
            if (sql.contains("count(")) {
                Assertions.assertEquals(1L, expected.get(0).get("total"));
            } else {
                Assertions.assertEquals("a", expected.get(0).get("type"));
            }
            StepVerifier.create(optimized.start(context.apply(true)).map(ReactorQLRecord::asMap), 0)
                    .thenRequest(1)
                    .expectNext(expected.get(0))
                    .verifyComplete();
        }
    }

    @Test
    void shouldKeepAggregateReadsOfGeneratedGroupMetadataOnRecordPath() {
        for (String expression : Arrays.asList("count(_group_by_key)",
                "count(distinct _group_by_key)", "sum(_group_by_key)")) {
            String sql = "select type," + expression
                    + " total from test group by _window(2),type";
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql).build();
            Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                    .contains("ASYNC_OR_STATEFUL[projection]"));
            boolean sourceField = expression.startsWith("sum");
            Flux<Map<String, Object>> rows = sourceField
                    ? Flux.just(map("type", "a", "_group_by_key", 7),
                                map("type", "a", "_group_by_key", 8))
                    : Flux.just(row("a", 1), row("a", 2));
            Object total;
            if (sourceField) {
                total = 15D;
            } else {
                total = expression.contains("distinct") ? 1L : 2L;
            }
            Map<String, Object> expected = map("type", "a", "total", total);
            // Values already present on the source take priority over generated Record metadata.
            StepVerifier.create(legacy.start(rows)).expectNext(expected).verifyComplete();
            StepVerifier.create(optimized.start(rows), 0).thenRequest(1)
                    .expectNext(expected).verifyComplete();
        }
    }

    @Test
    void shouldPreservePerKeyWindowWhenKeyPrecedesWindow() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select type,count(1) total,sum(score) sum "
                             + "from test group by type,_window(2)")
                .build();

        List<Map<String, Object>> result = query
                .start(Flux.just(row("a", 1),
                                 row("b", 10),
                                 row("a", 3),
                                 row("b", 20),
                                 row("a", 5)))
                .collectList()
                .block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("a", result.get(0).get("type"));
        Assertions.assertEquals(4D, result.get(0).get("sum"));
        Assertions.assertEquals("b", result.get(1).get("type"));
        Assertions.assertEquals(30D, result.get(1).get("sum"));
        Assertions.assertEquals("a", result.get(2).get("type"));
        Assertions.assertEquals(5D, result.get(2).get("sum"));
    }

    @Test
    void shouldDrainIncompletePerKeyWindowsInOrderAndReleaseOnCancelOrError() {
        ReactorQL query = ReactorQL.builder()
                                   .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 64)
                                   .sql("select type,count(1) total from test group by type,_window(2)")
                                   .build();
        List<Map<String, Object>> rows = new ArrayList<>();
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int index = 0; index < 32; index++) {
            String key = "key-" + index;
            rows.add(row(key, index));
            expected.add(map("type", key, "total", 1L));
        }
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Map<String, Object>> source = Flux.deferContextual(context -> {
            Assertions.assertEquals("visible", context.get("marker"));
            subscriptions.incrementAndGet();
            return Flux.fromIterable(rows);
        });

        StepVerifier.create(query.start(source)
                                 .contextWrite(context -> context.put("marker", "visible")), 0)
                    .thenRequest(1)
                    .expectNextMatches(expected::contains)
                    .thenCancel()
                    .verify();
        StepVerifier.create(query.start(source).collectList()
                                 .contextWrite(context -> context.put("marker", "visible")), 0)
                    .thenRequest(1)
                    .assertNext(results -> {
                        Assertions.assertEquals(expected.size(), results.size());
                        Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(results));
                    })
                    .verifyComplete();
        RuntimeException failure = new RuntimeException("per-key source failed");
        StepVerifier.create(query.start(Flux.concat(source, Flux.error(failure)))
                                 .contextWrite(context -> context.put("marker", "visible")))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
        Assertions.assertEquals(3, subscriptions.get());
    }

    @Test
    void shouldFilterSingleGroupPerKeyWindowsWithoutChangingDemandOrOrder() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select type,count(1) total from test "
                                                + "group by type,_window(2) having total > 1")
                                   .build();
        Flux<Map<String, Object>> rows = Flux.just(row("a", 1), row("b", 2),
                                                  row("a", 3), row("c", 4), row("b", 5));

        StepVerifier.create(query.start(rows), 0)
                    .thenRequest(1)
                    .expectNext(map("type", "a", "total", 2L))
                    .thenRequest(1)
                    .expectNext(map("type", "b", "total", 2L))
                    .thenRequest(1)
                    .verifyComplete();
    }

    @Test
    void shouldLimitActiveKeysInNativeGroupingLayer() {
        ReactorQL query = ReactorQL.builder()
                                   .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 2)
                                   .sql("select type,count(1) total from test "
                                                + "group by type,_window(2)")
                                   .build();

        StepVerifier.create(query.start(Flux.just(row("a", 1), row("b", 2), row("c", 3))))
                    .expectErrorMatches(error -> error instanceof ReactorQLException
                            && error.getMessage().contains(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS))
                    .verify();
    }

    @Test
    void shouldApplyHavingAfterIncrementalAggregation() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select type,count(1) total from test "
                             + "group by _window(4),type having total > 1")
                .build();

        query.start(Flux.just(row("a", 1), row("a", 2), row("b", 3), row("c", 4)))
             .as(StepVerifier::create)
             .assertNext(result -> {
                 Assertions.assertEquals("a", result.get("type"));
                 Assertions.assertEquals(2L, result.get("total"));
             })
             .verifyComplete();
    }

    @Test
    void shouldFuseCompletionBasedKeyedAggregationWithoutGroupedFlux() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select type,count(1) total,sum(score) sum from test group by type")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("ASYNC_OR_STATEFUL[projection]"));

        query.start(Flux.just(row("a", 1), row("b", 2), row("a", 3)))
             .collectList()
             .as(StepVerifier::create)
             .assertNext(result -> {
                 Assertions.assertEquals(2, result.size());
                 Assertions.assertEquals(2L, result.get(0).get("total"));
                 Assertions.assertEquals(4D, result.get(0).get("sum"));
                 Assertions.assertEquals(1L, result.get(1).get("total"));
             })
             .verifyComplete();
    }

    @Test
    void shouldFuseGlobalProcessingTimeWindow() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select count(1) total from test group by _window('250ms')")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("ASYNC_OR_STATEFUL[projection]"));

        StepVerifier.withVirtualTime(() -> query
                .start(Flux.interval(Duration.ofMillis(100)).take(5))
                .collectList())
                    .thenAwait(Duration.ofSeconds(1))
                    .assertNext(results -> {
                        long count = results
                                .stream()
                                .mapToLong(result -> ((Number) result.get("total")).longValue())
                                .sum();
                        Assertions.assertEquals(5L, count);
                        Assertions.assertTrue(results.size() >= 2);
                    })
                    .verifyComplete();
    }

    @Test
    void shouldExposeTimeWindowZeroDemandTradeoffForConcatMapPrefetch() {
        TimeWindowBackpressureProbe prefetchOne = probeTimeWindowBackpressure(1);
        Assertions.assertEquals(Long.MAX_VALUE, prefetchOne.requested.get());
        Assertions.assertEquals(7, prefetchOne.emitted.get());
        Assertions.assertTrue(prefetchOne.cancelled.get());
        Assertions.assertNull(prefetchOne.sourceError.get());
        Assertions.assertTrue(Exceptions.isOverflow(prefetchOne.downstreamError.get()));
        Assertions.assertFalse(prefetchOne.downstreamComplete.get());

        TimeWindowBackpressureProbe prefetchZero = probeTimeWindowBackpressure(0);
        Assertions.assertEquals(0L, prefetchZero.requested.get());
        Assertions.assertEquals(0, prefetchZero.emitted.get());
        Assertions.assertFalse(prefetchZero.cancelled.get());
        // FluxWindowBoundary cannot emit its initial window without demand, so it cancels before either source subscribes.
        Assertions.assertNull(prefetchZero.sourceError.get());
        Assertions.assertTrue(Exceptions.isOverflow(prefetchZero.downstreamError.get()));
        Assertions.assertEquals("Could not emit buffer due to lack of requests",
                                prefetchZero.downstreamError.get().getMessage());
        Assertions.assertFalse(prefetchZero.downstreamComplete.get());
    }

    @Test
    void shouldKeepNativeTimeWindowLifecycleAtZeroResultDemand() {
        for (boolean defaultPlan : Arrays.asList(false, true)) {
            ReactorQL query = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, defaultPlan)
                    .sql("select count(1) total from test group by _window('100ms')").build();
            TimeWindowBackpressureProbe probe = new TimeWindowBackpressureProbe();
            // Native flatMap prepares closed-window results before downstream requests them;
            // it does not share the removed concatMap(prefetch=1) overflow boundary.
            StepVerifier.withVirtualTime(() -> query.start(timeWindowProbeSource(probe))
                    .doOnError(probe.downstreamError::set)
                    .doOnComplete(() -> probe.downstreamComplete.set(true)), 0)
                    .thenAwait(Duration.ofMillis(349))
                    .thenCancel().verify();
            Assertions.assertEquals(Long.MAX_VALUE, probe.requested.get());
            Assertions.assertEquals(13, probe.emitted.get());
            Assertions.assertTrue(probe.cancelled.get());
            Assertions.assertNull(probe.sourceError.get());
            Assertions.assertNull(probe.downstreamError.get());
            Assertions.assertFalse(probe.downstreamComplete.get());
        }
    }

    @Test
    void shouldEmitClosedTimeWindowsOneAtATimeOnDemand() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select count(1) total from test group by _window('250ms')")
                .build();

        StepVerifier.withVirtualTime(() -> query.start(Flux.interval(Duration.ofMillis(100)).take(5)), 0)
                    .thenRequest(1)
                    .thenAwait(Duration.ofMillis(250))
                    .expectNext(map("total", 2L))
                    .thenRequest(1)
                    .thenAwait(Duration.ofMillis(500))
                    .expectNext(map("total", 2L))
                    .thenRequest(1)
                    .expectNext(map("total", 1L))
                    .verifyComplete();
    }

    @Test
    void shouldCancelTimeWindowSourceAfterDeliveringClosedWindow() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select count(1) total from test group by _window('100ms')")
                .build();
        TimeWindowBackpressureProbe probe = new TimeWindowBackpressureProbe();

        StepVerifier.withVirtualTime(() -> query.start(timeWindowProbeSource(probe).take(5)), 0)
                    .thenRequest(1)
                    .thenAwait(Duration.ofMillis(100))
                    .expectNext(map("total", 3L))
                    .thenCancel()
                    .verify();

        // The source may be requested eagerly by Reactor's time-window operator, but downstream
        // cancellation after a delivered window must still reach the active source subscription.
        Assertions.assertEquals(Long.MAX_VALUE, probe.requested.get());
        Assertions.assertEquals(3, probe.emitted.get());
        Assertions.assertTrue(probe.cancelled.get());
        Assertions.assertNull(probe.sourceError.get());
    }

    @Test
    void shouldPropagateTimeWindowErrorAndContext() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select count(1) total from test group by _window('100ms')")
                .build();
        AtomicBoolean contextVisible = new AtomicBoolean();
        RuntimeException failure = new RuntimeException("time window source failed");

        StepVerifier.withVirtualTime(() -> query.start(Flux.deferContextual(context -> {
                                            contextVisible.set("visible".equals(context.get("marker")));
                                            return Flux.concat(Mono.delay(Duration.ofMillis(25)),
                                                               Mono.error(failure));
                                        }))
                                        .contextWrite(context -> context.put("marker", "visible")))
                    .thenAwait(Duration.ofMillis(25))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
        Assertions.assertTrue(contextVisible.get());
    }

    private static TimeWindowBackpressureProbe probeTimeWindowBackpressure(int prefetch) {
        TimeWindowBackpressureProbe probe = new TimeWindowBackpressureProbe();
        String expectedMessage = prefetch == 0
                ? "Could not emit buffer due to lack of requests"
                : "Could not create new window due to lack of requests";

        StepVerifier.withVirtualTime(() -> timeWindowProbeResult(probe, prefetch), 0)
                    .thenAwait(prefetch == 0 ? Duration.ZERO : Duration.ofMillis(349))
                    .expectErrorMatches(error -> Exceptions.isOverflow(error)
                            && expectedMessage.equals(error.getMessage()))
                    .verify();
        return probe;
    }

    private static Flux<Long> timeWindowProbeResult(TimeWindowBackpressureProbe probe, int prefetch) {
        return timeWindowProbeSource(probe)
                   .window(Duration.ofMillis(100))
                   .concatMap(window -> window.count().flux(), prefetch)
                   .doOnError(probe.downstreamError::set)
                   .doOnComplete(() -> probe.downstreamComplete.set(true));
    }

    private static Flux<Long> timeWindowProbeSource(TimeWindowBackpressureProbe probe) {
        return Flux.interval(Duration.ofMillis(25))
                   .doOnRequest(probe.requested::addAndGet)
                   .doOnNext(ignore -> probe.emitted.incrementAndGet())
                   .doOnError(probe.sourceError::set)
                   .doOnCancel(() -> probe.cancelled.set(true));
    }

    private static final class TimeWindowBackpressureProbe {
        private final AtomicLong requested = new AtomicLong();
        private final AtomicInteger emitted = new AtomicInteger();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean downstreamComplete = new AtomicBoolean();
        private final AtomicReference<Throwable> sourceError = new AtomicReference<>();
        private final AtomicReference<Throwable> downstreamError = new AtomicReference<>();
    }

    @Test
    void shouldBoundActiveGroupState() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 2)
                .sql("select type,count(1) total from test group by _window(100),type")
                .build();

        query.start(Flux.just(row("a", 1), row("b", 2), row("c", 3)))
             .as(StepVerifier::create)
             .expectErrorMatches(error -> error instanceof ReactorQLException
                     && error.getMessage().contains(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS))
             .verify();
    }

    @Test
    void shouldKeepStatePerSubscriptionAndHonorCancellation() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select count(1) total from test group by _window(2)")
                .build();

        Flux<Map<String, Object>> first = query.start(Flux.range(0, 6));
        Flux<Map<String, Object>> second = query.start(Flux.range(0, 4));

        StepVerifier.create(Flux.zip(first, second))
                    .expectNextCount(2)
                    .verifyComplete();

        AtomicBoolean cancelled = new AtomicBoolean();
        Flux<Map<String, Object>> cancellable = query.start(
                Flux.range(0, 100).hide().doOnCancel(() -> cancelled.set(true))
        );
        StepVerifier.create(cancellable, 0)
                    .thenRequest(1)
                    .assertNext(result -> Assertions.assertEquals(2L, result.get("total")))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldUseCompactStateForGroupOnlyProjection() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select type,count(1) total from test group by _window(2),type")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("ASYNC_OR_STATEFUL[projection]"));

        query.start(Flux.just(row("a", 1), row("a", 2)))
             .as(StepVerifier::create)
             .expectNext(map("type", "a", "total", 2L))
             .verifyComplete();
    }

    @Test
    void shouldRetainLastRowForNonGroupProjectionCompatibility() {
        ReactorQL query = ReactorQL
                .builder()
                .sql("select type,score,count(1) total from test group by _window(2),type")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("ASYNC_OR_STATEFUL[projection]"));

        query.start(Flux.just(row("a", 1), row("a", 2)))
             .as(StepVerifier::create)
             .expectNext(map("type", "a", "score", 2, "total", 2L))
             .verifyComplete();
    }

    @Test
    void shouldPreserveNativeClosedWindowDemandAndCancellation() {
        AtomicInteger results = new AtomicInteger();
        ValueAggMapFeature probe = new ValueAggMapFeature() {
            @Override
            public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(
                    Expression expression,
                    ReactorQLMetadata metadata) {
                return flux -> flux.count().map(ignored -> results.incrementAndGet()).cast(Object.class).flux();
            }

            @Override
            public String getId() {
                return FeatureId.ValueAggMap.of("probe").getId();
            }
        };
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL
                .builder()
                .feature(probe)
                .sql("select type,probe(score) value from test group by _window(3),type")
                .build();

        StepVerifier.create(
                        query.start(Flux
                                            .concat(Flux.just(row("a", 1),
                                                                   row("b", 2),
                                                                   row("c", 3)),
                                                    Flux.never())
                                            .doOnCancel(() -> cancelled.set(true))),
                        0
                )
                .thenRequest(1)
                .assertNext(value -> {
                    Assertions.assertEquals("a", value.get("type"));
                    Assertions.assertEquals(1, results.get());
                })
                .thenCancel()
                .verify();

        Assertions.assertTrue(cancelled.get());
        Assertions.assertEquals(1, results.get());
    }

    @Test
    void shouldKeepNativeGlobalResultDemandAndSingleCalculation() {
        AtomicInteger results = new AtomicInteger();
        ValueAggMapFeature probe = new ValueAggMapFeature() {
            @Override
            public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(
                    Expression expression, ReactorQLMetadata metadata) {
                return flux -> flux.count().map(ignored -> results.incrementAndGet()).cast(Object.class).flux();
            }

            @Override
            public String getId() { return FeatureId.ValueAggMap.of("probe").getId(); }
        };
        ReactorQL query = ReactorQL.builder().feature(probe).sql("select probe(score) value from test").build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan().contains("ASYNC_OR_STATEFUL[projection]"));
        // Like the retained collect.flatMap implementation, one result is calculated after source
        // completion, but it is not emitted before demand and is not recalculated after request.
        StepVerifier.create(query.start(Flux.just(row("a", 1))), 0)
                    .then(() -> Assertions.assertEquals(1, results.get()))
                    .thenCancel().verify();
        Assertions.assertEquals(1, results.get());
        StepVerifier.create(query.start(Flux.just(row("a", 1))), 0)
                    .then(() -> Assertions.assertEquals(2, results.get()))
                    .thenRequest(1)
                    .expectNext(map("value", 2))
                    .verifyComplete();
        Assertions.assertEquals(2, results.get());
    }

    @Test
    void shouldFuseDistinctAggregateButKeepCheckpointFallback() {
        ReactorQL distinct = ReactorQL
                .builder()
                .sql("select count(distinct type) total from test group by _window(4)")
                .build();
        ReactorQL checkpoint = ReactorQL
                .builder()
                .setting("checkpoint", true)
                .sql("select count(1) total from test group by _window(4)")
                .build();
        ReactorQL disabled = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql("select count(1) total from test group by _window(4)")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) distinct)
                                       .describeExecutionPlan()
                                       .contains("ASYNC_OR_STATEFUL[projection]"));
        Assertions.assertTrue(((DefaultReactorQL) checkpoint)
                                       .describeExecutionPlan()
                                       .contains("ASYNC_OR_STATEFUL[projection]"));
        Assertions.assertTrue(((DefaultReactorQL) disabled)
                                       .describeExecutionPlan()
                                       .contains("ASYNC_OR_STATEFUL[projection]"));

        distinct.start(Flux.just(row("a", 1), row("a", 2), row("b", 3), row("b", 4)))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals(2L, result.get("total")))
                .verifyComplete();
    }

    @Test
    void shouldKeepExactCountResultsIsolatedPerGroupAndWindow() {
        Flux<Map<String, Object>> rows = Flux.just(
                row("a", 1), row("a", 1), row("b", 2),
                row("a", 3), row("b", 2), row("b", 4));
        for (String modifier : Arrays.asList("distinct", "unique")) {
            String sql = "select type,count(" + modifier + " score) total from test "
                    + "group by _window(3),type";
            ReactorQL fused = ReactorQL.builder().sql(sql).build();
            ReactorQL fallback = ReactorQL.builder()
                                          .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                          .sql(sql)
                                          .build();
            Assertions.assertTrue(((DefaultReactorQL) fused).describeExecutionPlan()
                    .contains("ASYNC_OR_STATEFUL[projection]"));
            List<Map<String, Object>> expected = fallback.start(rows).collectList().block();
            Assertions.assertNotNull(expected);
            Assertions.assertEquals(Arrays.asList(
                    map("type", "a", "total", "distinct".equals(modifier) ? 1L : 0L),
                    map("type", "b", "total", 1L),
                    map("type", "a", "total", 1L),
                    map("type", "b", "total", 2L)), expected);
            fused.start(rows)
                 .collectList()
                 .as(StepVerifier::create)
                 .expectNext(expected)
                 .verifyComplete();
            fused.start(rows)
                 .collectList()
                 .as(StepVerifier::create)
                 .expectNext(expected)
                 .verifyComplete();
        }
    }

    @Test
    void shouldKeepMultiValueExactCountOnPublisherPath() {
        FunctionMapFeature duplicate = new FunctionMapFeature(
                "duplicate_values", 1, 1,
                values -> values.flatMap(value -> Flux.just(value, value)));
        for (String modifier : Arrays.asList("distinct", "unique")) {
            ReactorQL query = ReactorQL.builder()
                                       .feature(duplicate)
                                       .sql("select count(" + modifier + " duplicate_values(score)) total "
                                                    + "from test group by _window(2)")
                                       .build();
            Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                    .contains("ASYNC_OR_STATEFUL[projection]"));
            query.start(Flux.just(row("a", 1), row("b", 2)))
                 .as(StepVerifier::create)
                 .expectNext(map("total", "distinct".equals(modifier) ? 2L : 0L))
                 .verifyComplete();
        }
    }

    @Test
    void shouldIgnoreMissingExactCountValuesAcrossRawAndRecordRows() {
        for (String modifier : Arrays.asList("distinct", "unique")) {
            ReactorQL query = ReactorQL.builder()
                                       .sql("select count(" + modifier + " score) total from test")
                                       .build();
            Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                    .contains("ASYNC_OR_STATEFUL[projection]"));
            StepVerifier.create(query.start(Flux.deferContextual(view -> {
                                         Assertions.assertEquals("visible", view.get("marker"));
                                         return Flux.just(row("a", 1), map("type", "missing"),
                                                          new ScoreRow(2), row("b", 1));
                                     }))
                                     .contextWrite(context -> context.put("marker", "visible")), 0)
                        .thenRequest(1)
                        .expectNext(map("total", "distinct".equals(modifier) ? 2L : 1L))
                        .verifyComplete();
        }
    }

    private static void assertAggregate(Map<String, Object> result,
                                        String type,
                                        long count,
                                        double sum,
                                        double avg,
                                        Object min,
                                        Object max) {
        Assertions.assertEquals(type, result.get("type"));
        Assertions.assertEquals(count, result.get("total"));
        Assertions.assertEquals(sum, result.get("sum"));
        Assertions.assertEquals(avg, result.get("avg"));
        Assertions.assertEquals(min, result.get("min"));
        Assertions.assertEquals(max, result.get("max"));
    }

    private static Map<String, Object> row(String type, int score) {
        Map<String, Object> row = new HashMap<>();
        row.put("type", type);
        row.put("score", score);
        return row;
    }

    private static Map<String, Object> map(Object... entries) {
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            values.put(String.valueOf(entries[i]), entries[i + 1]);
        }
        return values;
    }
}
