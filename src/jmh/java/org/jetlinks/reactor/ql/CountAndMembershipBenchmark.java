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

/** Complete compiled queries, immutable inputs, and independently checked output per subscription. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CountAndMembershipBenchmark {
    private static final int ROWS = 1024;

    @Param({"count1", "countStar", "in8", "in64", "in512", "integer512",
            "hit512", "string8", "string512", "numericText512", "mixed512", "control"})
    public String workload;

    private ReactorQL query;
    private Map<String, Object>[] rows;
    private long expectedCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        boolean count = workload.startsWith("count");
        boolean strings = workload.startsWith("string");
        boolean control = "control".equals(workload);
        int size = workload.endsWith("8") ? 8 : workload.endsWith("64") ? 64 : 512;
        String sql;
        if (count) {
            sql = "select count(" + ("count1".equals(workload) ? "1" : "*") + ") total from test";
        } else if (control) {
            sql = "select id from test";
        } else {
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < size; i++) {
                if (i != 0) values.append(',');
                if (strings) values.append("'key-").append(i).append('\'');
                else if ("mixed512".equals(workload) && i == size - 1) values.append("'").append(i).append('\'');
                else values.append(i);
            }
            sql = "select id from test where choice in (" + values + ")";
        }
        query = ReactorQL.builder().sql(sql).build();
        rows = new Map[ROWS];
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            long number = "hit512".equals(workload) && i % 2 == 0 ? i % 512 : 1_000_000L + i;
            Object value;
            if (strings) value = "key-" + number;
            else if ("numericText512".equals(workload)) value = Long.toString(number);
            else if ("integer512".equals(workload)) value = (int) number;
            else value = number;
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("choice", value);
            rows[i] = Collections.unmodifiableMap(row);
            if (!count && (control || ("hit512".equals(workload) && i % 2 == 0))) {
                expected.add(Collections.singletonMap("id", i));
            }
        }
        if (count) expected.add(Collections.singletonMap("total", (Object) (long) ROWS));
        if (!expected.equals(query.start(Flux.fromArray(rows)).collectList().block())) {
            throw new IllegalStateException("Values, order or types differ: " + workload);
        }
        expectedCount = expected.size();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void execute(Blackhole blackhole) {
        Long count = query.start(Flux.fromArray(rows)).count().block();
        if (count == null || count != expectedCount) {
            throw new IllegalStateException("Result count differs: " + workload);
        }
        blackhole.consume(count);
    }
}
