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
 * Fixed-width export fields on prebuilt telemetry events, including multi-character padding,
 * Unicode, empty pads and truncation. Units are input events; the direct mapper is only a
 * normal-data reference, not an oracle for the complete SQL/extension/reactive contract.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class StringPaddingBenchmark {

    private static final int INPUT_ROWS = 16_384;

    @Param({"16", "64"})
    public int fieldWidth;

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
            row.put("deviceId", sequence % 13 == 0 ? "" : "sensor-" + sequence);
            String label = sequence % 7 == 0 ? "温度传感器-" + sequence : "temperature-" + sequence;
            if (sequence % 11 == 0) {
                label += "-north-campus-building-a-floor-3-production-telemetry-export";
            }
            row.put("label", label);
            row.put("keyPad", sequence % 19 == 0 ? "" : sequence % 3 == 0 ? "-_" : "0");
            row.put("labelPad", sequence % 23 == 0 ? "" : sequence % 5 == 0 ? "·界" : " ");
            rows[sequence] = row;
        }
        query = ReactorQL.builder()
                .sql("select sequence,lpad(deviceId," + fieldWidth + ",keyPad) device_key,"
                     + "rpad(label," + fieldWidth + ",labelPad) export_label from telemetry")
                .build();
        control = ReactorQL.builder()
                .sql("select sequence,deviceId device_key,label export_label from telemetry")
                .build();
        assertResults(query::start, this::nativeRow);
        assertResults(source -> source.map(this::nativeRow), this::nativeRow);
        assertResults(control::start, StringPaddingBenchmark::propertyRow);
    }

    private void assertResults(Function<Flux<Map<String, Object>>, Flux<Map<String, Object>>> route,
                               Function<Map<String, Object>, Map<String, Object>> expected) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = route.apply(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        if (subscriptions.get() != 1 || actual == null || actual.size() != INPUT_ROWS) {
            throw new IllegalStateException("padding source/result cardinality mismatch");
        }
        for (int index = 0; index < INPUT_ROWS; index++) {
            Map<String, Object> result = actual.get(index);
            Map<String, Object> reference = expected.apply(rows[index]);
            if (!reference.equals(result) || !Integer.valueOf(index).equals(result.get("sequence"))) {
                throw new IllegalStateException("padding output/order mismatch at " + index);
            }
            for (Map.Entry<String, Object> entry : reference.entrySet()) {
                if (entry.getValue().getClass() != result.get(entry.getKey()).getClass()) {
                    throw new IllegalStateException("padding output type mismatch");
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlPadding(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativePadding(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(this::nativeRow), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void propertyControl(Blackhole blackhole) {
        consume(control.start(Flux.fromArray(rows)), blackhole);
    }

    private Map<String, Object> nativeRow(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", row.get("sequence"));
        String key = pad((String) row.get("deviceId"), (String) row.get("keyPad"), fieldWidth, true);
        String label = pad((String) row.get("label"), (String) row.get("labelPad"), fieldWidth, false);
        if (key != null) {
            result.put("device_key", key);
        }
        if (label != null) {
            result.put("export_label", label);
        }
        return result;
    }

    private static Map<String, Object> propertyRow(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", row.get("sequence"));
        result.put("device_key", row.get("deviceId"));
        result.put("export_label", row.get("label"));
        return result;
    }

    private static String pad(String value, String padding, int width, boolean left) {
        if (value.length() >= width) {
            return value.substring(0, width);
        }
        if (padding.isEmpty()) {
            return null;
        }
        StringBuilder result = new StringBuilder(width);
        int count = width - value.length();
        if (!left) {
            result.append(value);
        }
        for (int index = 0; index < count; index++) {
            result.append(padding.charAt(index % padding.length()));
        }
        if (left) {
            result.append(value);
        }
        return result.toString();
    }

    private static void consume(Flux<Map<String, Object>> result, Blackhole blackhole) {
        CountingSubscriber subscriber = new CountingSubscriber(blackhole);
        result.subscribe(subscriber);
        if (subscriber.failure != null) {
            throw new IllegalStateException("padding benchmark failed", subscriber.failure);
        }
        if (!subscriber.completed || subscriber.count != INPUT_ROWS) {
            throw new IllegalStateException("padding output count: " + subscriber.count);
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
