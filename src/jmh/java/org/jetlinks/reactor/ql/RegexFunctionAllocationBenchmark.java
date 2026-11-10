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

/** Shared compiled queries and independent subscriptions over immutable, mixed-match input rows. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class RegexFunctionAllocationBenchmark {
    private static final int ROWS = 1024;

    @Param({"different", "settings", "single", "dynamic", "functions"})
    public String scenario;

    private ReactorQL query;
    private Map<String, Object>[] rows;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String sql;
        switch (scenario) {
            case "different":
                sql = "select regexp_like(label,'^DEVICE$') a,regexp_like(label,'^(?:DEVICE)$') b from test";
                break;
            case "settings":
                sql = "select regexp_like(label,'^DEVICE$') a,regexp_like(label,'^DEVICE$') b from test";
                break;
            case "single": sql = "select regexp_like(label,'^DEVICE$') a from test"; break;
            case "dynamic": sql = "select regexp_like(label,pattern) a from test"; break;
            case "functions":
                sql = "select id,substring(label,1,3) a,starts_with(label,'DE') b from test";
                break;
            default: throw new IllegalArgumentException(scenario);
        }
        ReactorQL.Builder builder = ReactorQL.builder().sql(sql);
        if ("settings".equals(scenario)) {
            builder.setting("concurrency", 256);
        }
        query = builder.build();
        rows = new Map[ROWS];
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            String label = (i & 1) == 0 ? "DEVICE" : "SENSOR";
            String pattern = (i & 1) == 0 ? "^DEVICE$" : "^(?:SENSOR)$";
            Map<String, Object> input = new HashMap<>();
            input.put("id", i);
            input.put("label", label);
            input.put("pattern", pattern);
            rows[i] = Collections.unmodifiableMap(input);
            Map<String, Object> result = new HashMap<>();
            if ("functions".equals(scenario)) {
                result.put("id", i);
                result.put("a", label.substring(0, 3));
                result.put("b", label.startsWith("DE"));
            } else {
                boolean matched = "dynamic".equals(scenario) || (i & 1) == 0;
                result.put("a", matched);
                if ("different".equals(scenario) || "settings".equals(scenario)) {
                    result.put("b", matched);
                }
            }
            expected.add(result);
        }
        List<Map<String, Object>> actual = query.start(Flux.fromArray(rows)).collectList().block();
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Complete result mismatch: " + scenario);
        }
        for (int i = 0; i < ROWS; i++) {
            for (String key : expected.get(i).keySet()) {
                if (expected.get(i).get(key).getClass() != actual.get(i).get(key).getClass()) {
                    throw new IllegalStateException("Result type mismatch: " + scenario + ":" + key);
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
