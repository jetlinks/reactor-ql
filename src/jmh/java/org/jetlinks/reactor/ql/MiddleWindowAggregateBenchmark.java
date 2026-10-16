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
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 相同前缀与输入行数下测窗口中段的一/多后缀，以及无后缀负对照。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class MiddleWindowAggregateBenchmark {

    private static final int ROWS = 50_000;
    private static final int KEYS = ROWS / 2;

    @Param({"one", "two", "noSuffix"})
    public String shape;

    private ReactorQL query;
    private Map<String, Object>[] rows;
    private int expectedGroups;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String suffix = "noSuffix".equals(shape) ? "" : ",type";
        query = ReactorQL.builder()
                         .sql("select deviceId,count(1) total,sum(score) sum,avg(score) avg "
                                      + "from test group by deviceId,_window(3)" + suffix)
                         .build();
        rows = (Map<String, Object>[]) new Map[ROWS];
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> row = new HashMap<>(4);
            row.put("deviceId", index % KEYS);
            row.put("type", "two".equals(shape) ? index / KEYS : 0);
            row.put("score", index % 100);
            rows[index] = row;
        }
        expectedGroups = "two".equals(shape) ? ROWS : KEYS;
        List<Map<String, Object>> result = query.start(Flux.fromArray(rows)).collectList().block();
        if (result == null || result.size() != expectedGroups) {
            throw new IllegalStateException("分组数不正确: " + (result == null ? null : result.size()));
        }
        long count = result.stream().mapToLong(row -> ((Number) row.get("total")).longValue()).sum();
        if (count != ROWS) {
            throw new IllegalStateException("输入计数不正确: " + count);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void aggregate(Blackhole blackhole) {
        Long groups = query.start(Flux.fromArray(rows))
                           .doOnNext(blackhole::consume)
                           .count()
                           .block();
        if (groups == null || groups != expectedGroups) {
            throw new IllegalStateException("分组数不正确: " + groups);
        }
    }
}
