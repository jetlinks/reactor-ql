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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Nested SQL workloads for measuring lookup subscription behavior.
 *
 * <p>B1 deliberately uses an aggregate below a derived-table layer. Its expected sum proves that the
 * lookup is fully consumed rather than treated as a scalar first-row subquery. A matching
 * cache-disabled control retains one lookup subscription per outer row. B2 is a single-layer,
 * correlated negative control, so its lookup must be subscribed once for every outer row.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class NestedSqlWorkloadBenchmark {

    private static final int B1_OUTER_ROWS = 1_024;
    private static final int B1_LOOKUP_ROWS = 1_024;
    private static final long B1_LOOKUP_TOTAL = (long) B1_LOOKUP_ROWS * (B1_LOOKUP_ROWS + 1) / 2;
    private static final int B2_OUTER_ROWS = 4_096;
    private static final int B2_LOOKUP_ROWS = 256;
    private static final List<String> B1_COLUMNS = Arrays.asList("o.id", "lookup_total");
    private static final List<String> B2_COLUMNS = Arrays.asList("o.id", "lookup_value");

    private ReactorQL nestedUncorrelatedAggregate;
    private ReactorQL nestedUncorrelatedAggregateCacheDisabled;
    private ReactorQL correlatedSubquery;
    private Map<String, Object>[] b1OuterRows;
    private Map<String, Object>[] b1LookupRows;
    private Map<String, Object>[] b2OuterRows;
    private Map<String, Object>[] b2LookupRows;

    @Setup
    public void setup() {
        nestedUncorrelatedAggregate = ReactorQL.builder()
                                               .sql("select o.id,(select sum(n.value) total "
                                                       + "from (select value from lookup) n) lookup_total "
                                                       + "from outer_table o")
                                               .build();
        nestedUncorrelatedAggregateCacheDisabled = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, false)
                .sql("select o.id,(select sum(n.value) total "
                             + "from (select value from lookup) n) lookup_total "
                             + "from outer_table o")
                .build();
        correlatedSubquery = ReactorQL.builder()
                                       .sql("select o.id,(select value from lookup where lookup.id = o.id) lookup_value "
                                               + "from outer_table o")
                                       .build();
        b1OuterRows = createOuterRows(B1_OUTER_ROWS, B1_OUTER_ROWS);
        b1LookupRows = createAggregateLookupRows();
        b2OuterRows = createOuterRows(B2_OUTER_ROWS, B2_LOOKUP_ROWS);
        b2LookupRows = createCorrelatedLookupRows();

        verifyNestedUncorrelatedAggregate(nestedUncorrelatedAggregate, 1);
        verifyNestedUncorrelatedAggregate(nestedUncorrelatedAggregateCacheDisabled, B1_OUTER_ROWS);
        verifyCorrelatedSubquery();
    }

    @Benchmark
    @OperationsPerInvocation(B1_OUTER_ROWS)
    public void nestedUncorrelatedAggregate(Blackhole blackhole) {
        consume(nestedUncorrelatedAggregate.start(sources(b1OuterRows, b1LookupRows, null, null)),
                B1_OUTER_ROWS,
                blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(B1_OUTER_ROWS)
    public void nestedUncorrelatedAggregateCacheDisabled(Blackhole blackhole) {
        consume(nestedUncorrelatedAggregateCacheDisabled.start(
                        sources(b1OuterRows, b1LookupRows, null, null)),
                B1_OUTER_ROWS,
                blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(B2_OUTER_ROWS)
    public void correlatedSubqueryNegativeControl(Blackhole blackhole) {
        consume(correlatedSubquery.start(sources(b2OuterRows, b2LookupRows, null, null)),
                B2_OUTER_ROWS,
                blackhole);
    }

    private void verifyNestedUncorrelatedAggregate(ReactorQL query,
                                                   int expectedLookupSubscriptions) {
        AtomicInteger outerSubscriptions = new AtomicInteger();
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        List<Map<String, Object>> result = query
                .start(sources(b1OuterRows, b1LookupRows, outerSubscriptions, lookupSubscriptions))
                .collectList()
                .block();

        assertSubscriptions("B1 nested uncorrelated aggregate", outerSubscriptions, lookupSubscriptions,
                expectedLookupSubscriptions);
        if (result == null || result.size() != B1_OUTER_ROWS) {
            throw new IllegalStateException("B1 nested uncorrelated aggregate row count: "
                    + (result == null ? null : result.size()));
        }
        for (int index = 0; index < result.size(); index++) {
            Map<String, Object> row = result.get(index);
            assertFields(row, B1_COLUMNS, "B1");
            assertNumber(row.get("o.id"), index, "B1 o.id");
            Map<?, ?> aggregate = assertMap(row.get("lookup_total"), "B1 lookup_total");
            if (aggregate.size() != 1 || !aggregate.keySet().equals(Collections.singleton("total"))) {
                throw new IllegalStateException("B1 aggregate fields: " + aggregate.keySet());
            }
            assertNumber(aggregate.get("total"), B1_LOOKUP_TOTAL, "B1 lookup total");
        }
    }

    private void verifyCorrelatedSubquery() {
        AtomicInteger outerSubscriptions = new AtomicInteger();
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        List<Map<String, Object>> result = correlatedSubquery
                .start(sources(b2OuterRows, b2LookupRows, outerSubscriptions, lookupSubscriptions))
                .collectList()
                .block();

        assertSubscriptions("B2 correlated subquery", outerSubscriptions, lookupSubscriptions, B2_OUTER_ROWS);
        if (result == null || result.size() != B2_OUTER_ROWS) {
            throw new IllegalStateException("B2 correlated subquery row count: "
                    + (result == null ? null : result.size()));
        }
        for (int index = 0; index < result.size(); index++) {
            Map<String, Object> row = result.get(index);
            assertFields(row, B2_COLUMNS, "B2");
            int lookupId = index % B2_LOOKUP_ROWS;
            assertNumber(row.get("o.id"), lookupId, "B2 o.id");
            Map<?, ?> nested = assertMap(row.get("lookup_value"), "B2 lookup_value");
            if (nested.size() != 1 || !nested.keySet().equals(Collections.singleton("value"))) {
                throw new IllegalStateException("B2 nested fields: " + nested.keySet());
            }
            assertNumber(nested.get("value"), lookupValue(lookupId), "B2 lookup value");
        }
    }

    private static Function<String, Publisher<?>> sources(Map<String, Object>[] outerRows,
                                                           Map<String, Object>[] lookupRows,
                                                           AtomicInteger outerSubscriptions,
                                                           AtomicInteger lookupSubscriptions) {
        return name -> {
            if ("outer_table".equals(name)) {
                return source(outerRows, outerSubscriptions);
            }
            if ("lookup".equals(name)) {
                return source(lookupRows, lookupSubscriptions);
            }
            return Flux.empty();
        };
    }

    private static Flux<Map<String, Object>> source(Map<String, Object>[] rows, AtomicInteger subscriptions) {
        if (subscriptions == null) {
            return Flux.fromArray(rows);
        }
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createOuterRows(int count, int idModulo) {
        Map<String, Object>[] rows = new Map[count];
        for (int index = 0; index < count; index++) {
            Map<String, Object> row = new LinkedHashMap<>(1);
            row.put("id", index % idModulo);
            rows[index] = row;
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createAggregateLookupRows() {
        Map<String, Object>[] rows = new Map[B1_LOOKUP_ROWS];
        for (int index = 0; index < rows.length; index++) {
            rows[index] = Collections.<String, Object>singletonMap("value", index + 1);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createCorrelatedLookupRows() {
        Map<String, Object>[] rows = new Map[B2_LOOKUP_ROWS];
        for (int index = 0; index < rows.length; index++) {
            Map<String, Object> row = new LinkedHashMap<>(2);
            row.put("id", index);
            row.put("value", lookupValue(index));
            rows[index] = row;
        }
        return rows;
    }

    private static int lookupValue(int id) {
        return 10_000 + id * 7;
    }

    private static void assertSubscriptions(String scenario,
                                            AtomicInteger outerSubscriptions,
                                            AtomicInteger lookupSubscriptions,
                                            int expectedLookupSubscriptions) {
        if (outerSubscriptions.get() != 1 || lookupSubscriptions.get() != expectedLookupSubscriptions) {
            throw new IllegalStateException(scenario + " subscriptions: outer=" + outerSubscriptions.get()
                    + ", lookup=" + lookupSubscriptions.get() + ", expected outer=1, lookup="
                    + expectedLookupSubscriptions);
        }
    }

    private static void assertFields(Map<String, Object> row, List<String> expected, String scenario) {
        if (row.size() != expected.size() || !row.keySet().equals(new java.util.LinkedHashSet<>(expected))) {
            throw new IllegalStateException(scenario + " fields: " + row.keySet());
        }
    }

    private static Map<?, ?> assertMap(Object value, String column) {
        if (!(value instanceof Map)) {
            throw new IllegalStateException(column + " type: "
                    + (value == null ? null : value.getClass().getName()));
        }
        return (Map<?, ?>) value;
    }

    private static void assertNumber(Object value, long expected, String column) {
        if (!(value instanceof Number) || ((Number) value).longValue() != expected) {
            throw new IllegalStateException(column + ": " + value + ", expected=" + expected);
        }
    }

    private static void consume(Flux<Map<String, Object>> result, int expectedRows, Blackhole blackhole) {
        ResultSubscriber subscriber = result.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null) {
            throw new IllegalStateException("nested SQL benchmark failed", subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != expectedRows) {
            throw new IllegalStateException("nested SQL benchmark rows: " + subscriber.count
                    + ", expected=" + expectedRows);
        }
    }

    private static final class ResultSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole;
        private int count;
        private Throwable error;
        private boolean complete;

        private ResultSubscriber(Blackhole blackhole) {
            this.blackhole = blackhole;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            count++;
            blackhole.consume(value);
        }

        @Override
        protected void hookOnComplete() {
            complete = true;
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
        }
    }
}
