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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
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
 * Bounded device batches with repeated aggregate inputs and nested property access.
 * Query compilation and input allocation are outside measurement. Source completion closes
 * the ten-second window; this measures processing allocation, not sustained-stream live heap.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
public class NestedAggregateInputBenchmark {
    private static final int ROWS = 4096;

    @Param({"bracket", "bracketPath", "dynamic", "mixed", "dot"})
    public String shape;

    @Param({"double", "decimalText"})
    public String inputType;

    private ReactorQL query;
    private Flux<Map<String, Object>> source;

    @Setup(Level.Trial)
    public void setup() {
        source = createSource();
        query = createQuery();
        verifyResults();
    }

    private Flux<Map<String, Object>> createSource() {
        List<Map<String, Object>> rows = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            double number = 20.5 + (i / 64) % 4;
            Object value = "double".equals(inputType) ? Double.valueOf(number) : String.valueOf(number);
            Map<String, Object> properties = Collections.singletonMap("cpuSystemUsage", value);
            Map<String, Object> row = new HashMap<>();
            row.put("deviceId", "device-" + i % 64);
            row.put("metric", "cpuSystemUsage");
            row.put("properties", properties);
            row.put("payload", Collections.singletonMap("telemetry", Collections.singletonMap("properties", properties)));
            rows.add(row);
        }
        return Flux.fromIterable(rows);
    }

    private ReactorQL createQuery() {
        String value;
        switch (shape) {
            case "bracketPath": value = "this.payload['telemetry.properties.cpuSystemUsage']"; break;
            case "dynamic": value = "this.properties[this.metric]"; break;
            case "bracket": value = "this.properties['cpuSystemUsage']"; break;
            case "mixed": value = "this.payload.telemetry.properties['cpuSystemUsage']"; break;
            case "dot": value = "this.payload.telemetry.properties.cpuSystemUsage"; break;
            default: throw new IllegalArgumentException("Unknown shape: " + shape);
        }
        String argument = "cast(" + value + " as double)";
        return ReactorQL.builder().sql("select this.deviceId deviceId,avg(" + argument
                + ") avgValue,max(" + argument + ") maxValue,min(" + argument
                + ") minValue,count(1) total from device "
                + "where " + value + " is not null group by interval('10s'),this.deviceId having avgValue > 10").build();
    }

    private void verifyResults() {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = query.start(source.doOnSubscribe(ignored -> subscriptions.incrementAndGet()))
                .collectList().block();
        if (result == null || result.size() != 64 || subscriptions.get() != 1) {
            throw new IllegalStateException("Group or source-subscription oracle failed");
        }
        for (Map<String, Object> row : result) {
            if (!Double.valueOf(22).equals(row.get("avgValue"))
                    || !Double.valueOf(23.5).equals(row.get("maxValue"))
                    || !Double.valueOf(20.5).equals(row.get("minValue"))
                    || !Long.valueOf(64).equals(row.get("total"))) {
                throw new IllegalStateException("Aggregate oracle failed: " + row);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void aggregateInput(Blackhole blackhole) {
        query.start(source).doOnNext(blackhole::consume).blockLast();
    }
}
