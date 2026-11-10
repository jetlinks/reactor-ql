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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measures exact UNIQUE aggregation on reused event values with both repeated and singleton keys.
 * The source array is prebuilt so allocation samples belong to query execution, not input boxing.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class UniqueAggregateBenchmark {

    private static final int REPEATED_KEYS = 255;
    private static final int REPEATS_PER_KEY = 256;
    private static final int SINGLETON_KEYS = 256;
    private static final int ROWS = REPEATED_KEYS * REPEATS_PER_KEY + SINGLETON_KEYS;
    private static final int KEYED_ROWS = 10_000;

    private Integer[] rows;
    private Integer[] allUniqueRows;
    private Object[] keyedRows;
    private Object[] keyedRepeatedRows;
    private ReactorQL unique;
    private ReactorQL distinct;
    private ReactorQL publisherDistinct;
    private ReactorQL streamingDistinctSum;
    private ReactorQL scalarAggregates;
    private ReactorQL filteredScalarAggregates;
    private ReactorQL keyedUnique;
    private ReactorQL keyedDistinct;

    @Setup(Level.Trial)
    public void setup() {
        Integer[] keys = new Integer[REPEATED_KEYS + SINGLETON_KEYS];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = i;
        }
        rows = new Integer[ROWS];
        for (int i = 0; i < REPEATED_KEYS * REPEATS_PER_KEY; i++) {
            rows[i] = keys[i % REPEATED_KEYS];
        }
        for (int i = 0; i < SINGLETON_KEYS; i++) {
            rows[REPEATED_KEYS * REPEATS_PER_KEY + i] = keys[REPEATED_KEYS + i];
        }
        allUniqueRows = new Integer[ROWS];
        for (int i = 0; i < ROWS; i++) {
            allUniqueRows[i] = i;
        }
        unique = ReactorQL.builder().sql("select count(unique this) total from test").build();
        distinct = ReactorQL.builder().sql("select count(distinct this) total from test").build();
        publisherDistinct = ReactorQL.builder().sql("select distinct_count(this) total from test").build();
        streamingDistinctSum = ReactorQL.builder().sql("select sum(distinct this) total from test").build();
        assertTotal(unique, (long) SINGLETON_KEYS);
        assertTotal(distinct, (long) keys.length);
        assertGlobalAggregate(publisherDistinct, rows, (long) keys.length);
        assertGlobalAggregate(publisherDistinct, allUniqueRows, (long) ROWS);
        assertGlobalAggregate(streamingDistinctSum, rows, (long) keys.length * (keys.length - 1) / 2.0D);
        assertGlobalAggregate(streamingDistinctSum, allUniqueRows, (long) ROWS * (ROWS - 1) / 2.0D);

        String scalarSql = "select count(this) total,sum(this) sum,avg(this) avg,"
                + "min(this) min,max(this) max from test";
        scalarAggregates = ReactorQL.builder().sql(scalarSql).build();
        filteredScalarAggregates = ReactorQL.builder().sql(scalarSql + " where this >= 128").build();
        assertScalarAggregates(scalarAggregates, 0);
        assertScalarAggregates(filteredScalarAggregates, 128);

        keyedRows = new Object[KEYED_ROWS];
        keyedRepeatedRows = new Object[KEYED_ROWS * 2];
        for (int i = 0; i < KEYED_ROWS; i++) {
            Map<String, Object> event = new HashMap<>(2);
            event.put("deviceId", i);
            event.put("score", i & 1023);
            keyedRows[i] = event;
            keyedRepeatedRows[i * 2] = event;
            Map<String, Object> repeated = new HashMap<>(event);
            keyedRepeatedRows[i * 2 + 1] = repeated;
        }
        keyedUnique = ReactorQL.builder()
                               .sql("select deviceId,count(unique score) total from test group by deviceId")
                               .build();
        keyedDistinct = ReactorQL.builder()
                                 .sql("select deviceId,count(distinct score) total from test group by deviceId")
                                 .build();
        assertKeyed(keyedUnique, keyedRows, 1L);
        assertKeyed(keyedDistinct, keyedRows, 1L);
        assertKeyed(keyedUnique, keyedRepeatedRows, 0L);
        assertKeyed(keyedDistinct, keyedRepeatedRows, 1L);
    }

    private void assertTotal(ReactorQL query, long expected) {
        Map<String, Object> result = query.start(Flux.fromArray(rows)).blockLast();
        if (result == null || !Objects.equals(expected, result.get("total"))) {
            throw new IllegalStateException("Unexpected aggregate result: " + result + ", expected=" + expected);
        }
    }

    private void assertGlobalAggregate(ReactorQL query, Integer[] input, Object expected) {
        AtomicInteger subscriptions = new AtomicInteger();
        Map<String, Object> result = query
                .start(Flux.fromArray(input).doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                .blockLast();
        if (result == null || !Objects.equals(expected, result.get("total"))
                || subscriptions.get() != 1) {
            throw new IllegalStateException("Unexpected DISTINCT aggregate: " + result + ", expected=" + expected);
        }
    }

    private void assertKeyed(ReactorQL query, Object[] input, long expected) {
        Set<Object> seen = new HashSet<>();
        AtomicInteger subscriptions = new AtomicInteger();
        long emitted = query.start(Flux.fromArray(input)
                                       .doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                            .doOnNext(result -> {
                                if (!Objects.equals(expected, result.get("total"))
                                        || !seen.add(result.get("deviceId"))) {
                                    throw new IllegalStateException("Unexpected keyed aggregate result: " + result);
                                }
                            })
                            .count()
                            .block();
        if (emitted != KEYED_ROWS || seen.size() != KEYED_ROWS || subscriptions.get() != 1) {
            throw new IllegalStateException("Unexpected keyed aggregate count or source subscriptions");
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> countUniqueRepeated() {
        return unique.start(Flux.fromArray(rows)).blockLast();
    }

    private void assertScalarAggregates(ReactorQL query, int minimum) {
        long count = 0;
        double sum = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int value : rows) {
            if (value >= minimum) {
                count++;
                sum += value;
                min = Math.min(min, value);
                max = Math.max(max, value);
            }
        }
        Map<String, Object> expected = new HashMap<>();
        expected.put("total", count);
        expected.put("sum", sum);
        expected.put("avg", sum / count);
        expected.put("min", min);
        expected.put("max", max);
        Map<String, Object> result = query.start(Flux.fromArray(rows)).single().block();
        if (!expected.equals(result)) {
            throw new IllegalStateException("Unexpected scalar aggregate result: " + result);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> scalarAggregates() {
        return scalarAggregates.start(Flux.fromArray(rows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> filteredScalarAggregates() {
        return filteredScalarAggregates.start(Flux.fromArray(rows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> countDistinctRepeated() {
        return distinct.start(Flux.fromArray(rows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> publisherDistinctRepeated() {
        return publisherDistinct.start(Flux.fromArray(rows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> publisherDistinctAllUnique() {
        return publisherDistinct.start(Flux.fromArray(allUniqueRows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> streamingDistinctSumRepeated() {
        return streamingDistinctSum.start(Flux.fromArray(rows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public Map<String, Object> streamingDistinctSumAllUnique() {
        return streamingDistinctSum.start(Flux.fromArray(allUniqueRows)).blockLast();
    }

    @Benchmark
    @OperationsPerInvocation(KEYED_ROWS)
    public long keyedUniqueCount() {
        return keyedUnique.start(Flux.fromArray(keyedRows)).count().block();
    }

    @Benchmark
    @OperationsPerInvocation(KEYED_ROWS)
    public long keyedDistinctCount() {
        return keyedDistinct.start(Flux.fromArray(keyedRows)).count().block();
    }

    @Benchmark
    @OperationsPerInvocation(KEYED_ROWS * 2)
    public long keyedUniqueRepeatedCount() {
        return keyedUnique.start(Flux.fromArray(keyedRepeatedRows)).count().block();
    }

    @Benchmark
    @OperationsPerInvocation(KEYED_ROWS * 2)
    public long keyedDistinctRepeatedCount() {
        return keyedDistinct.start(Flux.fromArray(keyedRepeatedRows)).count().block();
    }
}
