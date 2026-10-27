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
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批读数位置选择的16列投影。JMH trial预建普通可修改／只读输入并校验完整结果，
 * 测量仍逐行计算，不预存输出、限定生产输入类型或调整生产资源边界。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class IndexedPropertyBenchmark {

    private static final int ROWS = 16_384;
    private static final int READINGS = 24;

    @Param({"mutable", "readonly"})
    public String inputKind;

    private Map<String, Object>[] rows;
    private ReactorQL query;
    private ReactorQL control;

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() {
        String common = "select sequence,deviceId,eventTime,region,site,battery,signal,active,firmware,";
        query = ReactorQL.builder().sql(common
                + "readings[0] first_value,readings[6] quarter_value,readings[12] middle_value,"
                + "readings[23] last_value,cast(readings[23] - readings[0] as long) delta,"
                + "abs(readings[12]) absolute_value,length(deviceId) name_length from telemetry").build();
        control = ReactorQL.builder().sql(common
                + "first first_value,quarter quarter_value,middle middle_value,last last_value,"
                + "storedDelta delta,absolute absolute_value,nameLength name_length from telemetry").build();
        rows = new Map[ROWS];
        for (int index = 0; index < ROWS; index++) {
            List<Integer> readings = new ArrayList<>();
            for (int reading = 0; reading < READINGS; reading++) {
                readings.add(index % 128 - 64 + reading);
            }
            Map<String, Object> row = new HashMap<>();
            row.put("sequence", index);
            row.put("deviceId", "device-" + (index & 255));
            row.put("eventTime", 1_791_331_200_000L + index);
            row.put("region", "region-" + (index & 7));
            row.put("site", "site-" + (index & 15));
            row.put("battery", index % 101);
            row.put("signal", -40 - index % 51);
            row.put("active", index % 5 != 0);
            row.put("firmware", "1." + (index & 3));
            row.put("readings", "readonly".equals(inputKind) ? Collections.unmodifiableList(readings) : readings);
            row.put("first", readings.get(0));
            row.put("quarter", readings.get(6));
            row.put("middle", readings.get(12));
            row.put("last", readings.get(23));
            row.put("storedDelta", (long) readings.get(23) - readings.get(0));
            row.put("absolute", Math.abs(readings.get(12).doubleValue()));
            row.put("nameLength", ((String) row.get("deviceId")).length());
            rows[index] = row;
        }
        AtomicInteger subscriptions = new AtomicInteger();
        verify(query.start(countedSource(subscriptions)));
        requireOneSubscription(subscriptions);
        subscriptions.set(0);
        verify(countedSource(subscriptions).map(IndexedPropertyBenchmark::resultRow));
        requireOneSubscription(subscriptions);
        subscriptions.set(0);
        verify(control.start(countedSource(subscriptions)));
        requireOneSubscription(subscriptions);
    }

    private Flux<Map<String, Object>> countedSource(AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    private static void requireOneSubscription(AtomicInteger subscriptions) {
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("indexed property source subscription mismatch");
        }
    }

    private void verify(Flux<Map<String, Object>> source) {
        // 只在trial setup收集这份固定有界输入；实际测量不保留输出行。
        List<Map<String, Object>> results = source.collectList().block();
        if (results == null || results.size() != ROWS) {
            throw new IllegalStateException("indexed property result count mismatch");
        }
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> expected = resultRow(rows[index]);
            Map<String, Object> actual = results.get(index);
            if (actual.size() != 16 || !expected.equals(actual)) {
                throw new IllegalStateException("indexed property full row/order mismatch: " + index);
            }
            for (String key : expected.keySet()) {
                if (expected.get(key).getClass() != actual.get(key).getClass()) {
                    throw new IllegalStateException("indexed property value type mismatch: " + key);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlIndexedProperties(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeIndexedProperties(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(IndexedPropertyBenchmark::resultRow), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void propertyControl(Blackhole blackhole) {
        consume(control.start(Flux.fromArray(rows)), blackhole);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> resultRow(Map<String, Object> row) {
        List<Integer> readings = (List<Integer>) row.get("readings");
        Map<String, Object> result = new HashMap<>();
        for (String key : new String[]{"sequence", "deviceId", "eventTime", "region", "site", "battery", "signal", "active", "firmware"}) {
            result.put(key, row.get(key));
        }
        result.put("first_value", readings.get(0));
        result.put("quarter_value", readings.get(6));
        result.put("middle_value", readings.get(12));
        result.put("last_value", readings.get(23));
        result.put("delta", (long) readings.get(23) - readings.get(0));
        result.put("absolute_value", Math.abs(readings.get(12).doubleValue()));
        result.put("name_length", ((String) row.get("deviceId")).length());
        return result;
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        long count = source.doOnNext(blackhole::consume).count().block();
        if (count != ROWS) {
            throw new IllegalStateException("indexed property row count mismatch: " + count);
        }
    }
}
