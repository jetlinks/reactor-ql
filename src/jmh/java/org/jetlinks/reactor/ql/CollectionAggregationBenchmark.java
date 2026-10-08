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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Isolates high-cardinality collection aggregation. Run with {@code -prof gc}
 * to compare allocation between the fused path and its Publisher-compatible path.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CollectionAggregationBenchmark {

    private static final int KEY_COUNT = 1_000;
    private static final int VALUES_PER_KEY = 10;
    private static final int ROWS = KEY_COUNT * VALUES_PER_KEY;
    private static final String COLLECT_LIST_SQL =
            "select type,collect_list(score,'label') values from test group by type";
    private static final String COUNT_SQL =
            "select type,count(1) total from test group by type";

    private ReactorQL collectListFastPath;
    private ReactorQL collectListPublisherPath;
    private ReactorQL countFastPath;
    private ReactorQL countPublisherPath;
    private Map<String, Object>[] rows;

    @Setup
    public void setup() {
        rows = createRows();
        collectListFastPath = ReactorQL.builder().sql(COLLECT_LIST_SQL).build();
        collectListPublisherPath = ReactorQL.builder()
                                             .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                             .sql(COLLECT_LIST_SQL)
                                             .build();
        countFastPath = ReactorQL.builder().sql(COUNT_SQL).build();
        countPublisherPath = ReactorQL.builder()
                                          .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                          .sql(COUNT_SQL)
                                          .build();

        assertCollectListEquivalent();
        assertCountEquivalent();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long collectListFastPath() {
        return consume(collectListFastPath, "collect_list fast path");
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long collectListPublisherPath() {
        return consume(collectListPublisherPath, "collect_list compatible path");
    }

    /**
     * A scalar aggregate control: it should not regress while collection aggregation is optimized.
     */
    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long countFastPathControl() {
        return consume(countFastPath, "count fast path");
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long countPublisherPathControl() {
        return consume(countPublisherPath, "count compatible path");
    }

    private void assertCollectListEquivalent() {
        List<Map<String, Object>> fast = collect(collectListFastPath, "collect_list fast path");
        List<Map<String, Object>> compatible = collect(collectListPublisherPath,
                                                        "collect_list compatible path");
        assertCollectListShape(fast);
        assertCollectListShape(compatible);
        if (!groupsByType(compatible).equals(groupsByType(fast))) {
            throw new IllegalStateException("collect_list 快慢路径输出不一致");
        }
    }

    private Map<String, Map<String, Object>> groupsByType(List<Map<String, Object>> groups) {
        Map<String, Map<String, Object>> byType = new LinkedHashMap<>(KEY_COUNT);
        for (Map<String, Object> group : groups) {
            Object type = group.get("type");
            if (!(type instanceof String) || byType.put((String) type, group) != null) {
                throw new IllegalStateException("聚合结果分组键不符合预期");
            }
        }
        return byType;
    }

    private void assertCountEquivalent() {
        List<Map<String, Object>> fast = collect(countFastPath, "count fast path");
        List<Map<String, Object>> compatible = collect(countPublisherPath, "count compatible path");
        assertCountShape(fast);
        assertCountShape(compatible);
        if (!groupsByType(compatible).equals(groupsByType(fast))) {
            throw new IllegalStateException("count 控制组快慢路径输出不一致");
        }
    }

    private void assertCountShape(List<Map<String, Object>> result) {
        if (result.size() != KEY_COUNT) {
            throw new IllegalStateException("count 控制组分组数量不符合预期");
        }
        for (Map<String, Object> value : result) {
            if (!(value.get("total") instanceof Long)
                    || ((Long) value.get("total")) != VALUES_PER_KEY) {
                throw new IllegalStateException("count 控制组数量或类型不符合预期");
            }
        }
    }

    private void assertCollectListShape(List<Map<String, Object>> result) {
        if (result.size() != KEY_COUNT) {
            throw new IllegalStateException("collect_list 分组数量不符合预期: " + result.size());
        }
        Map<String, Map<String, Object>> byType = new LinkedHashMap<>(KEY_COUNT);
        for (Map<String, Object> group : result) {
            Object type = group.get("type");
            if (!(type instanceof String) || byType.put((String) type, group) != null) {
                throw new IllegalStateException("collect_list 分组键不符合预期");
            }
        }
        for (int key = 0; key < KEY_COUNT; key++) {
            String type = type(key);
            Map<String, Object> group = byType.get(type);
            if (group == null || !(group.get("values") instanceof ArrayList)) {
                throw new IllegalStateException("collect_list 返回列表类型不符合预期: " + type);
            }
            List<?> values = (List<?>) group.get("values");
            if (values.size() != VALUES_PER_KEY) {
                throw new IllegalStateException("collect_list 列表长度不符合预期: " + type);
            }
            for (int ordinal = 0; ordinal < VALUES_PER_KEY; ordinal++) {
                Object row = values.get(ordinal);
                if (!(row instanceof LinkedHashMap)) {
                    throw new IllegalStateException("collect_list 元素类型不符合预期: " + type);
                }
                Map<?, ?> value = (Map<?, ?>) row;
                if (!List.of("score", "label").equals(new ArrayList<>(value.keySet()))
                        || !Integer.valueOf(ordinal).equals(value.get("score"))
                        || !(type + "-" + ordinal).equals(value.get("label"))) {
                    throw new IllegalStateException("collect_list 列表顺序或值不符合预期: " + type);
                }
            }
        }
    }

    private List<Map<String, Object>> collect(ReactorQL query, String scenario) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = query.start(replay(subscriptions)).collectList().block();
        if (result == null || subscriptions.get() != 1) {
            throw new IllegalStateException(scenario + " 的源订阅次数或终止信号不符合预期");
        }
        return result;
    }

    private long consume(ReactorQL query, String scenario) {
        AtomicInteger subscriptions = new AtomicInteger();
        Consumption consumption = query.start(replay(subscriptions))
                                     .reduce(new Consumption(), Consumption::add)
                                     .block();
        if (consumption == null || consumption.groups != KEY_COUNT || subscriptions.get() != 1) {
            throw new IllegalStateException(scenario + " 未产生完整输出或源被重复订阅");
        }
        return consumption.valueCount;
    }

    private Flux<Map<String, Object>> replay(AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createRows() {
        Map<String, Object>[] values = new Map[ROWS];
        int offset = 0;
        for (int key = 0; key < KEY_COUNT; key++) {
            String type = type(key);
            for (int ordinal = 0; ordinal < VALUES_PER_KEY; ordinal++) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("type", type);
                row.put("score", ordinal);
                row.put("label", type + "-" + ordinal);
                values[offset++] = row;
            }
        }
        return values;
    }

    private static String type(int key) {
        return String.format("type-%04d", key);
    }

    private static final class Consumption {
        private long groups;
        private long valueCount;

        private Consumption add(Map<String, Object> result) {
            groups++;
            Object values = result.get("values");
            if (values instanceof List) {
                valueCount += ((List<?>) values).size();
            } else {
                valueCount += ((Number) result.get("total")).longValue();
            }
            return this;
        }
    }
}
