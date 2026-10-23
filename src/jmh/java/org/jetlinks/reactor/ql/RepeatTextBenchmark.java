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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 展示查询中的动态标记／分隔线重复，原生路径仍逐行计算，不预存输出。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class RepeatTextBenchmark {

    private static final int ROWS = 16_384;
    private static final String[] MARKERS = {"", "=", "--", "evt: ", "告警 ", "状态\uD83D\uDE00", "a\uD800"};
    private ReactorQL query;
    private ReactorQL propertyControl;
    private Map<String, Object>[] rows;

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() {
        query = ReactorQL.builder()
                .sql("select sequence,deviceId device_id,repeat(marker,copies) display_cell from test")
                .build();
        propertyControl = ReactorQL.builder()
                .sql("select sequence,deviceId device_id,marker display_cell from test")
                .build();
        rows = new Map[ROWS];
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> row = new HashMap<>(4);
            row.put("sequence", index);
            row.put("deviceId", "device-" + (index & 255));
            row.put("marker", MARKERS[index % MARKERS.length]);
            row.put("copies", (index * 13) % 33);
            rows[index] = row;
        }
        AtomicInteger sqlSubscriptions = new AtomicInteger();
        verify(query.start(countedSource(sqlSubscriptions)), true);
        if (sqlSubscriptions.get() != 1) {
            throw new IllegalStateException("repeat SQL source subscription mismatch");
        }
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        verify(nativeProjection(countedSource(nativeSubscriptions)), true);
        if (nativeSubscriptions.get() != 1) {
            throw new IllegalStateException("repeat native source subscription mismatch");
        }
        AtomicInteger propertySubscriptions = new AtomicInteger();
        verify(propertyControl.start(countedSource(propertySubscriptions)), false);
        if (propertySubscriptions.get() != 1) {
            throw new IllegalStateException("repeat property source subscription mismatch");
        }
    }

    private Flux<Map<String, Object>> countedSource(AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    private void verify(Flux<Map<String, Object>> source, boolean repeat) {
        List<Map<String, Object>> results = source.collectList().block();
        if (results == null || results.size() != ROWS) {
            throw new IllegalStateException("repeat result count mismatch");
        }
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> expected = resultRow(rows[index], repeat);
            Map<String, Object> actual = results.get(index);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("repeat full row/order mismatch: " + index);
            }
            for (String key : expected.keySet()) {
                if (expected.get(key).getClass() != actual.get(key).getClass()) {
                    throw new IllegalStateException("repeat value type mismatch: " + key);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlRepeat(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeRepeat(Blackhole blackhole) {
        consume(nativeProjection(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void propertyControl(Blackhole blackhole) {
        consume(propertyControl.start(Flux.fromArray(rows)), blackhole);
    }

    private static Flux<Map<String, Object>> nativeProjection(Flux<Map<String, Object>> source) {
        return source.map(row -> resultRow(row, true));
    }

    private static Map<String, Object> resultRow(Map<String, Object> row, boolean repeat) {
        String text = (String) row.get("marker");
        if (repeat) {
            int count = (Integer) row.get("copies");
            StringBuilder builder = new StringBuilder(text.length() * count);
            for (int index = 0; index < count; index++) {
                builder.append(text);
            }
            text = builder.toString();
        }
        Map<String, Object> result = new HashMap<>(4);
        result.put("sequence", row.get("sequence"));
        result.put("device_id", row.get("deviceId"));
        result.put("display_cell", text);
        return result;
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        ResultSubscriber subscriber = source.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null || !subscriber.complete || subscriber.count != ROWS) {
            throw new IllegalStateException("repeat benchmark failed: " + subscriber.count, subscriber.error);
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
        protected void hookOnNext(Map<String, Object> value) {
            blackhole.consume(value);
            count++;
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
