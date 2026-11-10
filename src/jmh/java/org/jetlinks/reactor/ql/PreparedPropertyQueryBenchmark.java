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

/** Full query execution with live property reads; compiled plans and inputs are shared, results are consumed. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class PreparedPropertyQueryBenchmark {

    private static final int ROWS = 1024;

    @Param({"fixedCast", "nestedCast", "numericPath", "numericDotPath", "missing"})
    public String scenario;

    private ReactorQL query;
    private Map<String, Object>[] rows;
    private int expectedCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String sql;
        switch (scenario) {
            case "fixedCast": sql = "select this['amount::long'] v from test"; break;
            case "nestedCast": sql = "select this['payload.amount::double'] v from test"; break;
            case "numericPath": sql = "select this['readings.[0]'] first,this['readings.[15]'] last from test"; break;
            case "numericDotPath": sql = "select this['readings.0'] first,this['readings.15'] last from test"; break;
            case "missing": sql = "select id from test where missing_value>0"; break;
            default: throw new IllegalArgumentException(scenario);
        }
        query = ReactorQL.builder().sql(sql).build();
        rows = new Map[ROWS];
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", index);
            row.put("amount", (long) index);
            row.put("payload", Collections.singletonMap("amount", index + ".25"));
            List<Integer> readings = new ArrayList<>();
            for (int reading = 0; reading < 16; reading++) {
                readings.add(index + reading);
            }
            row.put("readings", Collections.unmodifiableList(readings));
            rows[index] = Collections.unmodifiableMap(row);
            Map<String, Object> result = new HashMap<>();
            switch (scenario) {
                case "fixedCast": result.put("v", (long) index); break;
                case "nestedCast": result.put("v", index + 0.25D); break;
                case "numericPath":
                case "numericDotPath":
                    result.put("first", index);
                    result.put("last", index + 15);
                    break;
                case "missing": continue;
                default: throw new IllegalArgumentException(scenario);
            }
            expected.add(result);
        }
        List<Map<String, Object>> actual = query.start(Flux.fromArray(rows)).collectList().block();
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Prepared property values or ordering differ: " + scenario);
        }
        for (int row = 0; row < expected.size(); row++) {
            for (String key : expected.get(row).keySet()) {
                if (expected.get(row).get(key).getClass() != actual.get(row).get(key).getClass()) {
                    throw new IllegalStateException("Prepared property result type differs: " + scenario);
                }
            }
        }
        expectedCount = expected.size();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void execute(Blackhole blackhole) {
        Long count = query.start(Flux.fromArray(rows)).doOnNext(blackhole::consume).count().block();
        if (count == null || count != expectedCount) {
            throw new IllegalStateException("Prepared property result count differs: " + scenario);
        }
        blackhole.consume(count);
    }
}
