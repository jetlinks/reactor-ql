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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Width scaling for normal mixed telemetry projections; every expression is evaluated independently.
 * The direct chain is a normal-data reference, not a substitute for SQL extension/error contracts.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class LargeProjectionBenchmark {

    private static final int INPUT_ROWS = 16_384;

    @Param({"16", "64", "128"})
    public int columnCount;

    private Map<String, Object>[] rows;
    private String[] readingNames;
    private String[] labelNames;
    private String[] aliases;
    private ReactorQL mixedProjection;
    private ReactorQL propertyProjection;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        int measurementCount = (columnCount + 1) / 4;
        readingNames = new String[measurementCount];
        labelNames = new String[measurementCount];
        for (int index = 0; index < measurementCount; index++) {
            readingNames[index] = "reading_" + index;
            labelNames[index] = "label_" + index;
        }
        aliases = new String[columnCount];
        aliases[0] = "sequence";
        aliases[1] = "device_id";
        StringBuilder mixedSql = new StringBuilder("select sequence,deviceId device_id");
        StringBuilder propertySql = new StringBuilder("select sequence,deviceId device_id");
        for (int index = 2; index < columnCount; index++) {
            int measurement = (index - 2) / 4;
            String reading = readingNames[measurement];
            aliases[index] = "column_" + index;
            String expression;
            switch ((index - 2) % 4) {
                case 0:
                    expression = reading;
                    break;
                case 1:
                    expression = reading + " * 1.5 + " + measurement;
                    break;
                case 2:
                    expression = "round(" + reading + "/3.0,2)";
                    break;
                default:
                    expression = "coalesce(" + labelNames[measurement] + ",deviceId)";
            }
            mixedSql.append(',').append(expression).append(' ').append(aliases[index]);
            propertySql.append(',').append(reading).append(' ').append(aliases[index]);
        }
        mixedProjection = ReactorQL.builder().sql(mixedSql.append(" from telemetry").toString()).build();
        propertyProjection = ReactorQL.builder().sql(propertySql.append(" from telemetry").toString()).build();
        rows = new Map[INPUT_ROWS];
        for (int sequence = 0; sequence < INPUT_ROWS; sequence++) {
            Map<String, Object> row = new HashMap<>();
            row.put("sequence", sequence);
            row.put("deviceId", "device-" + sequence);
            for (int measurement = 0; measurement < measurementCount; measurement++) {
                row.put(readingNames[measurement], sequence % 4096 - 2048 + measurement);
                row.put(labelNames[measurement], (sequence + measurement) % 5 == 0 ? null
                        : (sequence + measurement) % 11 == 0 ? ""
                        : "metric-" + measurement + '-' + sequence % 64);
            }
            rows[sequence] = row;
        }
        assertResults(mixedProjection, this::mixedRow);
        assertResults(propertyProjection, this::propertyRow);
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> nativeRows = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        }).map(this::mixedRow).collectList().block();
        assertCompleteRows(nativeRows, this::mixedRow);
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("large native projection source subscription mismatch");
        }
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlMixedProjection(Blackhole blackhole) {
        consume(mixedProjection.start(name -> Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeMixedProjection(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(this::mixedRow), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlPropertyProjectionControl(Blackhole blackhole) {
        consume(propertyProjection.start(name -> Flux.fromArray(rows)), blackhole);
    }

    private Map<String, Object> mixedRow(Map<String, Object> row) {
        Map<String, Object> result = newResult(row);
        for (int index = 2; index < columnCount; index++) {
            int measurement = (index - 2) / 4;
            Object value;
            switch ((index - 2) % 4) {
                case 0:
                    value = row.get(readingNames[measurement]);
                    break;
                case 1:
                    value = ((Number) row.get(readingNames[measurement])).doubleValue() * 1.5 + measurement;
                    break;
                case 2:
                    value = Math.round(((Number) row.get(readingNames[measurement])).doubleValue()
                            / 3.0 * 100.0) / 100.0;
                    break;
                default:
                    value = row.get(labelNames[measurement]);
                    if (value == null) {
                        value = row.get("deviceId");
                    }
            }
            result.put(aliases[index], value);
        }
        return result;
    }

    private Map<String, Object> propertyRow(Map<String, Object> row) {
        Map<String, Object> result = newResult(row);
        for (int index = 2; index < columnCount; index++) {
            result.put(aliases[index], row.get(readingNames[(index - 2) / 4]));
        }
        return result;
    }

    private Map<String, Object> newResult(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>(columnCount * 4 / 3 + 1);
        result.put(aliases[0], row.get("sequence"));
        result.put(aliases[1], row.get("deviceId"));
        return result;
    }

    private void assertResults(ReactorQL query, Function<Map<String, Object>, Map<String, Object>> expected) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = query.start(name -> Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        assertCompleteRows(result, expected);
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("large SQL projection source subscription mismatch");
        }
    }

    private void assertCompleteRows(List<Map<String, Object>> result,
                                    Function<Map<String, Object>, Map<String, Object>> expectedMapper) {
        if (result == null || result.size() != INPUT_ROWS) {
            throw new IllegalStateException("large projection cardinality mismatch");
        }
        for (int index = 0; index < INPUT_ROWS; index++) {
            Map<String, Object> expected = expectedMapper.apply(rows[index]);
            Map<String, Object> actual = result.get(index);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("large projection value/order mismatch at " + index
                        + ": " + actual + " != " + expected);
            }
            expected.forEach((key, value) -> {
                if (!value.getClass().equals(actual.get(key).getClass())) {
                    throw new IllegalStateException("large projection field type mismatch: " + key);
                }
            });
        }
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        ConsumingSubscriber subscriber = new ConsumingSubscriber(blackhole);
        source.subscribe(subscriber);
        if (subscriber.error != null) {
            throw new IllegalStateException("large projection query failed", subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != INPUT_ROWS) {
            throw new IllegalStateException("large projection did not finish every input event");
        }
    }

    private static final class ConsumingSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole;
        private int count;
        private boolean complete;
        private Throwable error;

        private ConsumingSubscriber(Blackhole blackhole) {
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
