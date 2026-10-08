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
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Native reducers remain incremental while retaining independent group and error lifecycles. */
class AggregateNativeLifecycleTest {

    @Test
    void activeNativeAggregateSourcesAreCancelledExactlyOnce() {
        for (String columns : Arrays.asList("count(1) total,sum(value) sum",
                "count(value) total,sum(value) sum,avg(value) avg,min(value) min,max(value) max")) {
            TestPublisher<Map<String, Object>> source = TestPublisher.create();
            AtomicInteger cancelled = new AtomicInteger();
            ReactorQL query = ReactorQL.builder().sql("select " + columns + " from events").build();
            StepVerifier.create(query.start(source.flux().doOnCancel(cancelled::incrementAndGet)), 0)
                    .thenRequest(1)
                    .then(() -> {
                        source.assertSubscribers(1);
                        source.next(Collections.singletonMap("value", 3));
                    })
                    .thenCancel().verify();
            source.assertCancelled();
            Assertions.assertEquals(1, cancelled.get(), columns);
        }
    }

    @Test
    void collectRowParameterFailuresKeepNativeRowRecoveryForScalarAndPublisherMappers() {
        for (String failingColumn : Arrays.asList("name", "value")) {
            for (boolean scalar : Arrays.asList(false, true)) {
                RuntimeException failure = new IllegalStateException("read " + failingColumn);
                Map<String, Object> bad = new HashMap<>();
                bad.put("name", "bad");
                bad.put("value", 1);
                Map<String, Object> good = new HashMap<>();
                good.put("name", "good");
                good.put("value", 2);
                ValueMapFeature property = new ValueMapFeature() {
                    @Override
                    public String getId() { return FeatureId.ValueMap.property.getId(); }

                    @Override
                    public Function<ReactorQLRecord, Publisher<?>> createMapper(
                            Expression expression, ReactorQLMetadata metadata) {
                        String name = ((Column) expression).getColumnName();
                        ScalarValueMapper reader = record -> {
                            if (record.getRecord() == bad && name.equals(failingColumn)) {
                                throw failure;
                            }
                            return ((Map<?, ?>) record.getRecord()).get(name);
                        };
                        return scalar ? reader : record -> reader.apply(record);
                    }
                };
                ReactorQL query = ReactorQL.builder().feature(property)
                        .sql("select collect_row(name,value) rows from events").build();
                AtomicInteger recovered = new AtomicInteger();
                // Parameter mapping is resumable per input record, not a collector failure.
                StepVerifier.create(query.start(Flux.just(bad, good)).onErrorContinue((error, value) -> {
                    Assertions.assertSame(failure, error);
                    Assertions.assertTrue(value instanceof ReactorQLRecord);
                    Assertions.assertSame(bad, ((ReactorQLRecord) value).getRecord());
                    recovered.incrementAndGet();
                }), 0)
                        .thenRequest(1)
                        .expectNext(Collections.singletonMap("rows", Collections.singletonMap("good", 2)))
                        .verifyComplete();
                Assertions.assertEquals(1, recovered.get(), failingColumn + "/" + scalar);
            }
        }
    }

    @Test
    void defaultPlanKeepsNativeReducerAndHierarchicalGroupRecoveryScopes() {
        List<String> groups = Arrays.asList("", " group by type", " group by _window(4)",
                " group by _window(4),type", " group by type,_window(4)",
                " group by _window(4),type,region");
        for (String columns : Arrays.asList("max(score) highest", "max(score) highest,count(1) total")) {
            for (String group : groups) {
                RuntimeException failure = new IllegalStateException("comparison failure");
                List<Map<String, Object>> rows = Arrays.asList(row("a", 1, failure), row("a", -2, failure),
                        row("a", 3, failure), row("a", 4, failure), row("b", 5, failure),
                        row("b", 6, failure), row("a", 7, failure), row("a", 8, failure));
                String sql = "select " + columns + " from events" + group;
                Outcome nativeRun = run(sql, rows, false);
                Outcome defaultRun = run(sql, rows, true);
                String scenario = sql;
                Assertions.assertEquals(nativeRun.values, defaultRun.values, scenario);
                Assertions.assertSame(nativeRun.error, defaultRun.error, scenario);
                Assertions.assertEquals(nativeRun.recovered, defaultRun.recovered, scenario);
                Assertions.assertEquals(nativeRun.consumed.get(), defaultRun.consumed.get(), scenario);
                Assertions.assertEquals(nativeRun.cancelled.get(), defaultRun.cancelled.get(), scenario);
            }
        }
    }

    @Test
    void defaultPlanReadsPublicErrorPolicyAtTheActualMergeFailure() {
        RuntimeException failure = new IllegalStateException("comparison failure");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger recovered = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .sql("select max(score) highest from events group by _window(2)").build();
        try {
            StepVerifier.create(query.start(Flux.just(row("a", -1, failure), row("a", -2, failure),
                            row("a", 3, failure), row("a", 4, failure))
                            .hide().doOnNext(ignore -> {
                                if (consumed.incrementAndGet() == 2) {
                                    Hooks.onNextError((error, value) -> {
                                        Assertions.assertSame(failure, error);
                                        Assertions.assertNull(value);
                                        recovered.incrementAndGet();
                                        return null;
                                    });
                                }
                            })), 0)
                    .thenRequest(1)
                    .assertNext(result -> {
                        Assertions.assertEquals(Collections.singleton("highest"), result.keySet());
                        Assertions.assertEquals(4, ((Score) result.get("highest")).value);
                    }).verifyComplete();
            Assertions.assertEquals(1, recovered.get());
            Assertions.assertEquals(4, consumed.get());
        } finally {
            Hooks.resetOnNextError();
        }
    }

    @Test
    void globalReducersPreserveSourceSnapshotFailuresAndNativeRecovery() {
        for (String columns : Arrays.asList("count(1) total", "sum(value) total", "max(value) highest")) {
            RuntimeException failure = new IllegalStateException("snapshot failure");
            AtomicInteger snapshots = new AtomicInteger();
            Map<String, Object> row = new SnapshotFailureMap(failure, snapshots);
            row.put("value", 3);
            ReactorQL query = ReactorQL.builder().sql("select " + columns + " from events").build();
            StepVerifier.create(query.start(Flux.just(row)), 0)
                    .thenRequest(1).expectErrorMatches(error -> error == failure).verify();
            Assertions.assertEquals(1, snapshots.get(), columns);

            AtomicInteger recovered = new AtomicInteger();
            StepVerifier.create(query.start(Flux.just(row)).onErrorContinue((error, value) -> {
                Assertions.assertSame(failure, error);
                Assertions.assertNull(value);
                recovered.incrementAndGet();
            })).verifyComplete();
            Assertions.assertEquals(1, recovered.get(), columns);
            Assertions.assertEquals(2, snapshots.get(), columns);
        }
    }

    private static Outcome run(String sql, List<Map<String, Object>> rows, boolean defaultPlan) {
        ReactorQL.Builder builder = ReactorQL.builder().sql(sql);
        if (!defaultPlan) builder.setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false);
        ReactorQL query = builder.build();
        Outcome outcome = new Outcome();
        // Eight-row finite fixtures exercise recovery without buffering an open source.
        StepVerifier.create(query.start(Flux.deferContextual(context -> {
                            Assertions.assertEquals("visible", context.get("marker"));
                            return Flux.fromIterable(rows).hide();
                        }).doOnNext(ignore -> outcome.consumed.incrementAndGet())
                                .doOnCancel(outcome.cancelled::incrementAndGet))
                        .onErrorContinue((error, value) -> {
                            Assertions.assertNull(value);
                            outcome.recovered.add(error);
                        }).doOnNext(outcome.values::add)
                        .onErrorResume(error -> {
                            outcome.error = error;
                            return Flux.empty();
                        }).contextWrite(context -> context.put("marker", "visible")))
                .thenConsumeWhile(ignore -> true).verifyComplete();
        // No ORDER BY: compare the full row multiset, including values and types.
        outcome.values.sort(Comparator.comparing(value -> new TreeMap<>(value).toString()));
        return outcome;
    }

    private static Map<String, Object> row(String type, int value, RuntimeException failure) {
        Map<String, Object> row = new HashMap<>();
        row.put("type", type);
        row.put("region", "site");
        row.put("score", new Score(value, failure));
        return row;
    }

    private static final class Outcome {
        final List<Map<String, Object>> values = new ArrayList<>();
        final List<Throwable> recovered = new ArrayList<>();
        final AtomicInteger consumed = new AtomicInteger();
        final AtomicInteger cancelled = new AtomicInteger();
        Throwable error;
    }

    public static final class Score implements Comparable<Score> {
        final int value;
        private final RuntimeException failure;
        Score(int value, RuntimeException failure) {
            this.value = value;
            this.failure = failure;
        }
        @Override public int compareTo(Score other) {
            if (value < 0 || other.value < 0) throw failure;
            return Integer.compare(value, other.value);
        }
        @Override public String toString() { return Integer.toString(value); }
    }

    private static final class SnapshotFailureMap extends HashMap<String, Object> {
        private final RuntimeException failure;
        private final AtomicInteger snapshots;
        SnapshotFailureMap(RuntimeException failure, AtomicInteger snapshots) {
            this.failure = failure;
            this.snapshots = snapshots;
        }
        @Override public void forEach(BiConsumer<? super String, ? super Object> action) {
            snapshots.incrementAndGet();
            throw failure;
        }
    }
}
