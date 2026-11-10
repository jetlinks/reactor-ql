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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Correlated VALUES source with two computed rows for each input event.
 * Counts outer input events; the direct computation is only a normal-data reference,
 * not an oracle for extension, asynchronous parameter or error-recovery semantics.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CorrelatedValuesBenchmark {

    private static final int INPUT_ROWS = 16_384;
    private static final String SQL = "select src.id id,v.adjusted adjusted,v.bucket bucket,v.label label "
            + "from events src cross join (select adjusted,bucket,label from (values "
            + "(src.temperature + 1,src.battery % 7,upper(src.name)),"
            + "(src.temperature - 1,src.battery % 5,lower(src.name))) "
            + "v0(adjusted,bucket,label)) v";

    private ReactorQL query;
    private Map<String, Object>[] rows;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        query = ReactorQL.builder().sql(SQL).build();
        rows = new Map[INPUT_ROWS];
        for (int i = 0; i < rows.length; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("temperature", (i % 160) - 40);
            row.put("battery", i % 101);
            row.put("name", "Device-" + i);
            rows[i] = row;
        }
        AtomicInteger sqlSubscriptions = new AtomicInteger();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        // Only setup collects; both fixtures have a fixed, known 2 * INPUT_ROWS bound.
        List<Map<String, Object>> sqlRows = query.start(input(sqlSubscriptions)).collectList().block();
        List<Map<String, Object>> nativeRows = nativeQuery(input(nativeSubscriptions)).collectList().block();
        if (sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || sqlRows == null || nativeRows == null
                || sqlRows.size() != INPUT_ROWS * 2 || !nativeRows.equals(sqlRows)) {
            throw new IllegalStateException("Correlated VALUES result or input subscription mismatch");
        }
        for (int i = 0; i < sqlRows.size(); i++) {
            Map<String, Object> actual = sqlRows.get(i);
            for (Map.Entry<String, Object> entry : nativeRows.get(i).entrySet()) {
                if (actual.get(entry.getKey()).getClass() != entry.getValue().getClass()) {
                    throw new IllegalStateException("Correlated VALUES result type mismatch: " + entry.getKey());
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlCorrelatedValues(Blackhole blackhole) {
        consume(query.start(input(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeCorrelatedValues(Blackhole blackhole) {
        consume(nativeQuery(input(null)), blackhole);
    }

    private Flux<Map<String, Object>> input(AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            if (subscriptions != null) {
                subscriptions.incrementAndGet();
            }
            return Flux.fromArray(rows);
        });
    }

    private static Flux<Map<String, Object>> nativeQuery(Flux<Map<String, Object>> input) {
        return input.concatMapIterable(row -> Arrays.asList(computedRow(row, true), computedRow(row, false)));
    }

    private static Map<String, Object> computedRow(Map<String, Object> source, boolean first) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", source.get("id"));
        row.put("adjusted", ((Number) source.get("temperature")).longValue() + (first ? 1 : -1));
        row.put("bucket", ((Number) source.get("battery")).longValue() % (first ? 7 : 5));
        String name = String.valueOf(source.get("name"));
        row.put("label", first ? name.toUpperCase(Locale.ENGLISH) : name.toLowerCase(Locale.ENGLISH));
        return row;
    }

    private static void consume(Flux<Map<String, Object>> result, Blackhole blackhole) {
        CountingSubscriber subscriber = new CountingSubscriber(blackhole);
        result.subscribe(subscriber);
        if (subscriber.failure != null) {
            throw new IllegalStateException("Correlated VALUES benchmark failed", subscriber.failure);
        }
        if (!subscriber.completed || subscriber.count != INPUT_ROWS * 2) {
            throw new IllegalStateException("Correlated VALUES output count: " + subscriber.count);
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
