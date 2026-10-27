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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Prepared inputs and complete, independently calculated outputs for common function workloads. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CommonFunctionOptimizationBenchmark {
    private static final int ROWS = 1024;

    @Param({"numericText", "numericNumber", "epoch", "instant", "regexMiss", "regexStatic",
            "regexDynamic", "regexHighCardinality", "control"})
    public String scenario;

    private ReactorQL query;
    private Map<String, Object>[] rows;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String projection;
        switch (scenario) {
            case "numericText":
            case "numericNumber":
                projection = "abs(value) a,sqrt(value) b,round(value,2) c,power(value,2) d";
                break;
            case "epoch":
            case "instant":
                projection = "year(value) a,to_millis(value) b,to_unixtime(value) c,to_iso_instant(value) d";
                break;
            case "regexMiss": projection = "regexp_replace(value,'^DEVICE$','SENSOR') a"; break;
            case "regexStatic": projection = "regexp_like(value,'^DEVICE$') a"; break;
            case "regexDynamic":
            case "regexHighCardinality": projection = "regexp_like(value,pattern) a"; break;
            case "control": projection = "value a"; break;
            default: throw new IllegalArgumentException(scenario);
        }
        query = ReactorQL.builder().sql("select " + projection + " from test").build();
        rows = new Map[ROWS];
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> input = new HashMap<>();
            Map<String, Object> output = new HashMap<>();
            if (scenario.startsWith("numeric")) {
                String text = "12." + (20 + i % 70);
                // Retain the generic BigDecimal conversion as the independent prior-value oracle.
                double number = new BigDecimal(text).doubleValue();
                input.put("value", "numericText".equals(scenario) ? text : Double.valueOf(text));
                output.put("a", Math.abs(number));
                output.put("b", Math.sqrt(number));
                output.put("c", Math.round(number * 100D) / 100D);
                output.put("d", Math.pow(number, 2D));
            } else if ("epoch".equals(scenario) || "instant".equals(scenario)) {
                long millis = 1_700_000_000_000L + i;
                Instant instant = Instant.ofEpochMilli(millis).plusNanos(654321);
                Date prior = "epoch".equals(scenario) ? new Date(millis) : Date.from(instant);
                input.put("value", "epoch".equals(scenario) ? Long.valueOf(millis) : instant);
                output.put("a", prior.toInstant().atZone(ZoneId.systemDefault()).getYear());
                output.put("b", prior.getTime());
                output.put("c", prior.getTime() / 1000D);
                output.put("d", prior.toInstant().toString());
            } else if ("regexMiss".equals(scenario)) {
                String text = "device-" + i + "-".repeat(200);
                input.put("value", text);
                output.put("a", text);
            } else if ("regexHighCardinality".equals(scenario)) {
                input.put("value", "DEVICE-" + i % 32);
                input.put("pattern", "^DEVICE-" + i % 32 + "$");
                output.put("a", true);
            } else if ("control".equals(scenario)) {
                input.put("value", i);
                output.put("a", i);
            } else {
                input.put("value", (i & 1) == 0 ? "DEVICE" : "SENSOR");
                input.put("pattern", (i & 1) == 0 ? "^DEVICE$" : "^(?:SENSOR)$");
                output.put("a", "regexDynamic".equals(scenario) || (i & 1) == 0);
            }
            rows[i] = Collections.unmodifiableMap(input);
            expected.add(output);
        }
        List<Map<String, Object>> actual = query.start(Flux.fromArray(rows)).collectList().block();
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Complete output mismatch: " + scenario);
        }
        for (int i = 0; i < ROWS; i++) {
            for (String key : expected.get(i).keySet()) {
                if (expected.get(i).get(key).getClass() != actual.get(i).get(key).getClass()) {
                    throw new IllegalStateException("Output type mismatch: " + scenario + ":" + key);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long execute(Blackhole blackhole) {
        return query.start(Flux.fromArray(rows)).doOnNext(blackhole::consume).count().block();
    }
}
