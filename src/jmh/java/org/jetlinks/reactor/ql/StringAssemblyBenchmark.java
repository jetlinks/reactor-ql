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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Realistic label/path projection with variadic string functions and collection parameters.
 * Units are input events. The direct chain is a normal-data reference, not an extension oracle.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class StringAssemblyBenchmark {

    private static final int INPUT_ROWS = 16_384;

    private Map<String, Object>[] rows;
    private ReactorQL query;
    private ReactorQL control;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        rows = new Map[INPUT_ROWS];
        for (int sequence = 0; sequence < INPUT_ROWS; sequence++) {
            Map<String, Object> row = new HashMap<>();
            row.put("sequence", sequence);
            row.put("deviceId", "sensor-" + (sequence % 512));
            row.put("region", "region-" + (sequence % 8));
            row.put("site", "site-" + (sequence % 16));
            row.put("kind", sequence % 2 == 0 ? "temperature" : "humidity");
            row.put("name", "reading-" + sequence);
            row.put("optional", sequence % 3 == 0 ? null : "label-" + sequence);
            row.put("tags", sequence % 5 == 0 ? Collections.emptyList()
                    : Arrays.asList("telemetry", "", "tag-" + (sequence % 4)));
            rows[sequence] = row;
        }
        query = ReactorQL.builder()
                .sql("select sequence,"
                        + "concat(deviceId,'@',region,'/',upper(kind),':',coalesce(optional,name),':',tags) label,"
                        + "concat_ws('/',site,deviceId,lower(kind),optional,tags) path from telemetry")
                .build();
        control = ReactorQL.builder().sql("select sequence,name from telemetry").build();
        assertResults(query, StringAssemblyBenchmark::expectedRow);
        assertResults(control, StringAssemblyBenchmark::controlRow);

        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> nativeRows = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        }).map(StringAssemblyBenchmark::expectedRow).collectList().block();
        assertCompleteRows(nativeRows, StringAssemblyBenchmark::expectedRow);
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("native string assembly subscribed more than once");
        }
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlStringAssembly(Blackhole blackhole) {
        consume(query.start(name -> Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeStringAssembly(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(StringAssemblyBenchmark::expectedRow), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void simpleProjectionControl(Blackhole blackhole) {
        consume(control.start(name -> Flux.fromArray(rows)), blackhole);
    }

    private void assertResults(ReactorQL sql, Function<Map<String, Object>, Map<String, Object>> expected) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = sql.start(name -> Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        assertCompleteRows(result, expected);
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("string assembly source must be subscribed exactly once");
        }
    }

    private void assertCompleteRows(List<Map<String, Object>> result,
                                    Function<Map<String, Object>, Map<String, Object>> expectedMapper) {
        if (result == null || result.size() != INPUT_ROWS) {
            throw new IllegalStateException("string assembly output cardinality mismatch");
        }
        for (int index = 0; index < INPUT_ROWS; index++) {
            Map<String, Object> expected = expectedMapper.apply(rows[index]);
            Map<String, Object> actual = result.get(index);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("string assembly value/order mismatch at " + index
                        + ": " + actual + " != " + expected);
            }
            expected.forEach((key, value) -> {
                if (!value.getClass().equals(actual.get(key).getClass())) {
                    throw new IllegalStateException("string assembly field type mismatch: " + key);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> expectedRow(Map<String, Object> row) {
        List<String> tags = (List<String>) row.get("tags");
        StringBuilder label = new StringBuilder();
        label.append(row.get("deviceId")).append('@').append(row.get("region")).append('/');
        label.append(String.valueOf(row.get("kind")).toUpperCase(Locale.ENGLISH)).append(':');
        label.append(row.get("optional") == null ? row.get("name") : row.get("optional")).append(':');
        for (String tag : tags) {
            label.append(tag);
        }
        StringJoiner path = new StringJoiner("/");
        path.add(String.valueOf(row.get("site"))).add(String.valueOf(row.get("deviceId")));
        path.add(String.valueOf(row.get("kind")).toLowerCase(Locale.ENGLISH));
        if (row.get("optional") != null) {
            path.add(String.valueOf(row.get("optional")));
        }
        tags.forEach(path::add);
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", row.get("sequence"));
        result.put("label", label.toString());
        result.put("path", path.toString());
        return result;
    }

    private static Map<String, Object> controlRow(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", row.get("sequence"));
        result.put("name", row.get("name"));
        return result;
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        ConsumingSubscriber subscriber = new ConsumingSubscriber(blackhole);
        source.subscribe(subscriber);
        if (subscriber.error != null) {
            throw new IllegalStateException("string assembly query failed", subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != INPUT_ROWS) {
            throw new IllegalStateException("string assembly query did not finish all input events");
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
