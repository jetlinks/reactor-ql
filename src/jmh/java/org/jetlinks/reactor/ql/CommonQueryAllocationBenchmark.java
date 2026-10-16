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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Shared compiled plans, immutable input, independent subscriptions; results are fully consumed. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CommonQueryAllocationBenchmark {
    private static final int ROWS = 1024;

    @Param({"select", "missing", "binary", "filter", "like", "conversions", "likeReject", "mixed8", "scalar8"})
    public String scenario;

    private ReactorQL query;
    private Map<String, Object>[] rows;
    private long expectedCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String sql;
        switch (scenario) {
            case "select": sql = "select id,value,label from test"; break;
            case "missing": sql = "select id from test where payload.missing > 1000"; break;
            case "binary": sql = "select value+1 a,value+2 b,value+3 c,value+4 d from test"; break;
            case "filter": sql = "select id from test where cast(text as double)>20 and lower(label)='sensor'"; break;
            case "like": sql = "select id from test where lower(label) like 'sen%' or cast(text as double)<0"; break;
            case "conversions": sql = "select id,cast(text as double) v,lower(label) name from test"; break;
            case "likeReject": sql = "select id from test where lower(label) like 'missing%'"; break;
            case "mixed8": sql = "select f0 a,f1 b,f2 c,f3 d,f4 e,f5 f,f6 g,cast(f7 as double) h from test"; break;
            case "scalar8": sql = "select f0 a,f1 b,f2 c,f3 d,f4 e,f5 f,f6 g,f7 h from test"; break;
            default: throw new IllegalArgumentException(scenario);
        }
        query = ReactorQL.builder().sql(sql).build();
        rows = new Map[ROWS];
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int index = 0; index < ROWS; index++) {
            int value = index % 100;
            Map<String, Object> row = new HashMap<>();
            row.put("id", index);
            row.put("value", value);
            row.put("text", value + ".25");
            row.put("label", "SENSOR");
            row.put("payload", Collections.singletonMap("value", value));
            for (int field = 0; field < 8; field++) {
                row.put("f" + field, (value + field) + ".25");
            }
            rows[index] = Collections.unmodifiableMap(row);
            Map<String, Object> result = new HashMap<>();
            switch (scenario) {
                case "missing": continue;
                case "likeReject": continue;
                case "filter": if (value < 20) continue;
                case "like": result.put("id", index); break;
                case "binary":
                    result.put("a", value + 1L);
                    result.put("b", value + 2L);
                    result.put("c", value + 3L);
                    result.put("d", value + 4L);
                    break;
                case "conversions":
                    result.put("id", index);
                    result.put("v", Double.parseDouble(value + ".25"));
                    result.put("name", "SENSOR".toLowerCase(Locale.getDefault()));
                    break;
                case "mixed8":
                case "scalar8":
                    for (int field = 0; field < 8; field++) {
                        Object converted = field == 7 && "mixed8".equals(scenario)
                                ? (Object) Double.parseDouble((value + field) + ".25")
                                : (value + field) + ".25";
                        result.put(String.valueOf((char) ('a' + field)), converted);
                    }
                    break;
                default:
                    result.put("id", index);
                    result.put("value", value);
                    result.put("label", "SENSOR");
            }
            expected.add(result);
        }
        if (!expected.equals(query.start(Flux.fromArray(rows)).collectList().block())) {
            throw new IllegalStateException("Values, types, ordering or missing-field behavior differ: " + scenario);
        }
        expectedCount = expected.size();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void execute(Blackhole blackhole) {
        Long count = query.start(Flux.fromArray(rows)).count().block();
        if (count == null || count != expectedCount) {
            throw new IllegalStateException("Result count changed: " + scenario);
        }
        blackhole.consume(count);
    }
}
