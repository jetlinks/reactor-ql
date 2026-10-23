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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 只隔离多异步列的编排开销；两种方法消费完全相同的冷 Publisher。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class AsyncProjectionCompositionBenchmark {

    private static final int ROWS = 20_000;
    private static final Object EMPTY = new Object();

    @Param({"2", "3"})
    public int columnCount;

    private List<Integer> columns;
    private String[] names;

    @Setup
    public void setup() {
        columns = new ArrayList<>(columnCount);
        names = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns.add(i);
            names[i] = "column_" + i;
        }
        Map<String, Object> baseline = flatMapAndSortRow(42).block();
        Map<String, Object> candidate = zipDelayErrorRow(42).block();
        if (!baseline.equals(candidate)) {
            throw new IllegalStateException("多异步列编排结果不一致");
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long flatMapAndSort() {
        return consume(Flux.range(0, ROWS).flatMap(this::flatMapAndSortRow));
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long zipDelayError() {
        return consume(Flux.range(0, ROWS).flatMap(this::zipDelayErrorRow));
    }

    private Mono<Map<String, Object>> flatMapAndSortRow(int row) {
        return Flux
                .fromIterable(columns)
                .flatMapDelayError(column -> Mono
                                           .from(asyncValue(row, column))
                                           .map(value -> new IndexedValue(column, value)),
                                   columnCount,
                                   columnCount)
                .collectSortedList(Comparator.comparingInt(value -> value.index))
                .map(values -> {
                    Map<String, Object> result = new HashMap<>(4);
                    result.put("id", row);
                    for (IndexedValue value : values) {
                        result.put(names[value.index], value.value);
                    }
                    return result;
                });
    }

    private Mono<Map<String, Object>> zipDelayErrorRow(int row) {
        Mono<?>[] sources = new Mono<?>[columnCount];
        for (int i = 0; i < sources.length; i++) {
            sources[i] = Mono.<Object>from(asyncValue(row, i)).defaultIfEmpty(EMPTY);
        }
        return Mono.zipDelayError(values -> {
            Map<String, Object> result = new HashMap<>(4);
            result.put("id", row);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != EMPTY) {
                    result.put(names[i], values[i]);
                }
            }
            return result;
        }, sources);
    }

    private Publisher<Integer> asyncValue(int row, int column) {
        return Flux.just(row + column);
    }

    private static long consume(Flux<Map<String, Object>> source) {
        CountingSubscriber subscriber = source.subscribeWith(new CountingSubscriber());
        if (subscriber.error != null) {
            throw new IllegalStateException(subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != ROWS) {
            throw new IllegalStateException("同步基准未产生完整结果");
        }
        return subscriber.hash;
    }

    private static final class IndexedValue {
        private final int index;
        private final Object value;

        private IndexedValue(int index, Object value) {
            this.index = index;
            this.value = value;
        }
    }

    private static final class CountingSubscriber extends BaseSubscriber<Map<String, Object>> {
        private long count;
        private long hash;
        private Throwable error;
        private boolean complete;

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            count++;
            hash += value.hashCode();
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
