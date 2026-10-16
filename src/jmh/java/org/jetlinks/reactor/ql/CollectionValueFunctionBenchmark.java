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
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collection-valued telemetry functions, measured per input event rather than per reading.
 * The direct calculation is a normal-data reference, not an extension/error-scope oracle.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CollectionValueFunctionBenchmark {

    private static final int INPUT_ROWS = 16_384;
    private static final int READINGS = 24;

    private ReactorQL rowsToArray;
    private ReactorQL rowToArray;
    private ReactorQL arrayLength;
    private Map<String, Object>[] rows;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        rows = new Map[INPUT_ROWS];
        for (int id = 0; id < INPUT_ROWS; id++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            List<Map<String, Object>> readings = new ArrayList<>();
            for (int reading = 0; reading < READINGS; reading++) {
                Map<String, Object> entry = new HashMap<>();
                entry.put("value", reading == 0 && id % 7 == 0 ? null : id + reading);
                readings.add(entry);
            }
            row.put("readings", readings);
            row.put("firstReading", readings.get(0));
            row.put("lastReading", readings.get(READINGS - 1));
            rows[id] = row;
        }
        rowsToArray = ReactorQL.builder()
                               .sql("select id,rows_to_array(readings) readings from telemetry")
                               .build();
        rowToArray = ReactorQL.builder()
                              .sql("select id,row_to_array(firstReading,lastReading) readings from telemetry")
                              .build();
        arrayLength = ReactorQL.builder()
                               .sql("select id,array_len(readings) length from telemetry")
                               .build();
        assertResults(rowsToArray, false, false);
        assertResults(rowToArray, true, false);
        assertResults(arrayLength, false, true);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlRowsToArray(Blackhole blackhole) {
        consume(query(rowsToArray), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlRowToArray(Blackhole blackhole) {
        consume(query(rowToArray), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeRowsToArray(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(row -> expectedRow(row, false, false)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void arrayLengthControl(Blackhole blackhole) {
        consume(query(arrayLength), blackhole);
    }

    private Flux<Map<String, Object>> query(ReactorQL query) {
        return query.start(name -> Flux.fromArray(rows));
    }

    private void assertResults(ReactorQL query, boolean endpoints, boolean length) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = query
                .start(name -> Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.fromArray(rows);
                }))
                .collectList()
                .block();
        if (subscriptions.get() != 1 || actual == null || actual.size() != INPUT_ROWS) {
            throw new IllegalStateException("Collection function input/output cardinality mismatch");
        }
        for (int index = 0; index < INPUT_ROWS; index++) {
            Map<String, Object> result = actual.get(index);
            Map<String, Object> expected = expectedRow(rows[index], endpoints, length);
            if (!expected.equals(result) || !(result.get("id") instanceof Integer)) {
                throw new IllegalStateException("Collection function result mismatch at " + index
                        + ": expected=" + expected + ", actual=" + result);
            }
            if (length) {
                if (!(result.get("length") instanceof Long)) {
                    throw new IllegalStateException("Array length type mismatch");
                }
            } else {
                if (!(result.get("readings") instanceof List)) {
                    throw new IllegalStateException("Collection function result must be a List");
                }
                for (Object value : (List<?>) result.get("readings")) {
                    if (!(value instanceof Integer)) {
                        throw new IllegalStateException("Reading type mismatch");
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> expectedRow(Map<String, Object> row, boolean endpoints, boolean length) {
        List<Map<String, Object>> readings = (List<Map<String, Object>>) row.get("readings");
        Map<String, Object> result = new HashMap<>();
        result.put("id", row.get("id"));
        if (length) {
            result.put("length", (long) readings.size());
        } else {
            List<Object> values = new ArrayList<>();
            for (int index = 0; index < readings.size(); index++) {
                if (!endpoints || index == 0 || index == readings.size() - 1) {
                    Object value = readings.get(index).get("value");
                    if (value != null) {
                        values.add(value);
                    }
                }
            }
            result.put("readings", values);
        }
        return result;
    }

    private static void consume(Flux<Map<String, Object>> result, Blackhole blackhole) {
        CountingSubscriber subscriber = new CountingSubscriber(blackhole);
        result.subscribe(subscriber);
        if (subscriber.failure != null) {
            throw new IllegalStateException("Collection function benchmark failed", subscriber.failure);
        }
        if (!subscriber.completed || subscriber.count != INPUT_ROWS) {
            throw new IllegalStateException("Collection function output count: " + subscriber.count);
        }
    }

    private static final class CountingSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole;
        private int count;
        private boolean completed;
        private Throwable failure;

        private CountingSubscriber(Blackhole blackhole) {
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
            completed = true;
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            failure = throwable;
        }
    }
}
