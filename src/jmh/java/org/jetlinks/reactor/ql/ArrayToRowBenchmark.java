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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Turns batched device properties into an attribute Map. Units are input events.
 * The direct mapper is only a reference for this normal input, not an extension oracle.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class ArrayToRowBenchmark {

    private static final int INPUT_ROWS = 4_096;
    private static final int PROPERTY_COUNT = 64;

    private Map<String, Object>[] rows;
    private ReactorQL nestedFields;
    private ReactorQL simpleFields;
    private ReactorQL castFields;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String[] names = new String[PROPERTY_COUNT];
        for (int index = 0; index < names.length; index++) {
            names[index] = "property-" + index;
        }
        rows = new Map[INPUT_ROWS];
        for (int id = 0; id < INPUT_ROWS; id++) {
            List<Map<String, Object>> properties = new ArrayList<>();
            for (int index = 0; index < PROPERTY_COUNT; index++) {
                String name = index == 0 && id % 5 == 0 ? null : names[index];
                Integer value = index == 1 && id % 7 == 0 ? null : id + index;
                Map<String, Object> property = new HashMap<>();
                property.put("identity", Collections.singletonMap("name", name));
                property.put("measurement", Collections.singletonMap("value", value));
                property.put("name", name);
                property.put("value", value);
                properties.add(property);
            }
            Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            row.put("properties", properties);
            rows[id] = row;
        }
        nestedFields = ReactorQL.builder()
                .sql("select id,array_to_row(properties,'identity.name','measurement.value') attributes from telemetry")
                .build();
        simpleFields = ReactorQL.builder()
                .sql("select id,array_to_row(properties,'name','value') attributes from telemetry")
                .build();
        castFields = ReactorQL.builder()
                .sql("select id,array_to_row(properties,'identity.name','measurement.value::int') attributes from telemetry")
                .build();
        assertResults(nestedFields);
        assertResults(simpleFields);
        assertResults(castFields);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlNestedFields(Blackhole blackhole) {
        consume(nestedFields.start(name -> Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlSimpleFieldsControl(Blackhole blackhole) {
        consume(simpleFields.start(name -> Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlNestedCastFields(Blackhole blackhole) {
        consume(castFields.start(name -> Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeNestedFields(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(ArrayToRowBenchmark::expectedRow), blackhole);
    }

    private void assertResults(ReactorQL query) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = query.start(name -> Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        if (subscriptions.get() != 1 || actual == null || actual.size() != INPUT_ROWS) {
            throw new IllegalStateException("array_to_row source/result cardinality mismatch");
        }
        for (int index = 0; index < INPUT_ROWS; index++) {
            Map<String, Object> result = actual.get(index);
            if (!expectedRow(rows[index]).equals(result) || !(result.get("id") instanceof Integer)
                    || !(result.get("attributes") instanceof Map)) {
                throw new IllegalStateException("array_to_row result mismatch at " + index);
            }
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) result.get("attributes")).entrySet()) {
                if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof Integer)) {
                    throw new IllegalStateException("array_to_row attribute type mismatch");
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> expectedRow(Map<String, Object> row) {
        Map<Object, Object> attributes = new HashMap<>();
        for (Map<String, Object> property : (List<Map<String, Object>>) row.get("properties")) {
            Object name = ((Map<?, ?>) property.get("identity")).get("name");
            Object value = ((Map<?, ?>) property.get("measurement")).get("value");
            if (name != null && value != null) {
                attributes.put(name, value);
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put("id", row.get("id"));
        result.put("attributes", attributes);
        return result;
    }

    private static void consume(Flux<Map<String, Object>> result, Blackhole blackhole) {
        CountingSubscriber subscriber = new CountingSubscriber(blackhole);
        result.subscribe(subscriber);
        if (subscriber.failure != null) {
            throw new IllegalStateException("array_to_row benchmark failed", subscriber.failure);
        }
        if (!subscriber.completed || subscriber.count != INPUT_ROWS) {
            throw new IllegalStateException("array_to_row output count: " + subscriber.count);
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
