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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 真实复合键增量聚合：相同 50,000 条预构造输入，分别覆盖唯一键与重复键。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CompositeKeyAggregateBenchmark {

    private static final int ROWS = 50_000;

    @Param({"unique", "repeated"})
    public String keyShape;

    @Param({"map", "bean"})
    public String inputShape;

    private ReactorQL query;
    private Object[] rows;
    private int expectedGroups;

    @Setup
    public void setup() {
        query = ReactorQL.builder()
                         .sql("select product,device,count(1) total,sum(score) sum,"
                                      + "avg(score) avg,max(score) max "
                                      + "from test group by product,device")
                         .build();
        rows = new Object[ROWS];
        Map<List<Integer>, ExpectedGroup> expected = new HashMap<>();
        for (int i = 0; i < ROWS; i++) {
            int product = "unique".equals(keyShape) ? i / 4 : i % 32;
            int device = "unique".equals(keyShape) ? i % 4 : (i / 32) % 8;
            int score = i % 100;
            expected.computeIfAbsent(Arrays.asList(product, device), ignore -> new ExpectedGroup())
                    .add(score);
            if ("bean".equals(inputShape)) {
                rows[i] = new DeviceEvent(product, device, score);
                continue;
            }
            Map<String, Object> row = new HashMap<>(4);
            row.put("product", product);
            row.put("device", device);
            row.put("score", score);
            rows[i] = row;
        }
        expectedGroups = "unique".equals(keyShape) ? ROWS : 256;
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> output = query.start(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        if (subscriptions.get() != 1 || output == null || output.size() != expectedGroups) {
            throw new IllegalStateException("复合键聚合输出数量不符: "
                                                    + (output == null ? null : output.size()));
        }
        long count = output.stream().mapToLong(row -> ((Number) row.get("total")).longValue()).sum();
        if (count != ROWS) {
            throw new IllegalStateException("复合键聚合输入计数不符: " + count);
        }
        Set<String> columns = new HashSet<>(Arrays.asList("product", "device", "total", "sum", "avg", "max"));
        for (Map<String, Object> result : output) {
            List<Integer> key = Arrays.asList((Integer) result.get("product"), (Integer) result.get("device"));
            ExpectedGroup group = expected.remove(key);
            if (group == null || !result.keySet().equals(columns)) {
                throw new IllegalStateException("复合键聚合字段／键不正确: " + result);
            }
            assertValue(result, "total", group.count);
            assertValue(result, "sum", group.sum);
            assertValue(result, "avg", group.sum / group.count);
            assertValue(result, "max", group.max);
        }
        if (!expected.isEmpty()) {
            throw new IllegalStateException("复合键聚合缺少输出组");
        }
    }

    private static void assertValue(Map<String, Object> result, String column, Object expected) {
        Object actual = result.get(column);
        if (actual == null || actual.getClass() != expected.getClass() || !actual.equals(expected)) {
            throw new IllegalStateException("复合键聚合值／类型不正确: " + column
                    + " actual=" + actual + " expected=" + expected);
        }
    }

    /** Setup-only oracle; no expected state or precomputed aggregate is used by the measurement. */
    private static final class ExpectedGroup {
        private long count;
        private double sum;
        private int max = Integer.MIN_VALUE;

        private void add(int score) {
            count++;
            sum += score;
            max = Math.max(max, score);
        }
    }

    /** Ordinary bean source; SQL performs getter invocation and primitive boxing during measurement. */
    public static final class DeviceEvent {
        private final int product;
        private final int device;
        private final int score;

        public DeviceEvent(int product, int device, int score) {
            this.product = product;
            this.device = device;
            this.score = score;
        }

        public int getProduct() { return product; }
        public int getDevice() { return device; }
        public int getScore() { return score; }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void aggregate(Blackhole blackhole) {
        Long count = query.start(Flux.fromArray(rows))
                          .doOnNext(blackhole::consume)
                          .count()
                          .block();
        if (count == null || count != expectedGroups) {
            throw new IllegalStateException("复合键聚合输出数量不符: " + count);
        }
    }
}
