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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/** Same finite input and page for LIMIT, TOP and FETCH; setup verifies values before measuring. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class PaginationQueryBenchmark {

    private static final int ROWS = 2048;
    private static final int PAGE_SIZE = 8;

    @Param({"limit", "top", "fetch"})
    public String dialect;

    private ReactorQL query;
    private List<Map<String, Object>> rows;

    @Setup
    public void setup() {
        String sql;
        switch (dialect) {
            case "limit": sql = "select sort_value from test order by sort_value limit 8"; break;
            case "top": sql = "select top (8) sort_value from test order by sort_value"; break;
            case "fetch": sql = "select sort_value from test order by sort_value fetch first 8 rows only"; break;
            default: throw new IllegalArgumentException(dialect);
        }
        query = ReactorQL.builder().sql(sql).build();
        rows = new ArrayList<>(ROWS);
        List<Map<String, Object>> expected = new ArrayList<>(PAGE_SIZE);
        for (int rank = 0; rank < ROWS; rank++) {
            Map<String, Object> row = Collections.singletonMap("sort_value", rank);
            rows.add(row);
            if (rank < PAGE_SIZE) {
                expected.add(row);
            }
        }
        Collections.shuffle(rows, new Random(42));
        if (!expected.equals(query.start(Flux.fromIterable(rows)).collectList().block())) {
            throw new IllegalStateException("Ordered page values or types changed: " + dialect);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void execute(Blackhole blackhole) {
        Long count = query.start(Flux.fromIterable(rows)).count().block();
        if (count == null || count != PAGE_SIZE) {
            throw new IllegalStateException("Ordered page count changed: " + dialect);
        }
        blackhole.consume(count);
    }
}
