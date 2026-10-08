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
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises {@code writeGroupKey} through normal SQL planning, rather than its helper alone.
 *
 * <p>Each query has a count window equal to the bounded input, so each invocation releases its
 * aggregation state on completion. The one-dimensional cases are negative controls: no prior
 * group key exists when the key is written. Two- and three-dimensional cases force later keys
 * to append to a key published by an earlier group dimension.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class GroupKeySqlBenchmark {

    private static final int INPUT_ROWS = 4_096;

    private GroupCase oneDimension;
    private GroupCase twoDimensions;
    private GroupCase threeDimensions;

    @Setup
    public void setup() {
        oneDimension = createCase("region", 1, 64, 1, 1);
        twoDimensions = createCase("region,site", 2, 16, 16, 1);
        threeDimensions = createCase("region,site,device", 3, 8, 8, 8);

        verify(oneDimension, oneDimension.fastPath);
        verify(oneDimension, oneDimension.publisherPath);
        verify(twoDimensions, twoDimensions.fastPath);
        verify(twoDimensions, twoDimensions.publisherPath);
        verify(threeDimensions, threeDimensions.fastPath);
        verify(threeDimensions, threeDimensions.publisherPath);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public int oneDimensionFastPath(Blackhole blackhole) {
        return execute(oneDimension, oneDimension.fastPath, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public int oneDimensionPublisherPath(Blackhole blackhole) {
        return execute(oneDimension, oneDimension.publisherPath, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public int twoDimensionsFastPath(Blackhole blackhole) {
        return execute(twoDimensions, twoDimensions.fastPath, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public int twoDimensionsPublisherPath(Blackhole blackhole) {
        return execute(twoDimensions, twoDimensions.publisherPath, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public int threeDimensionsFastPath(Blackhole blackhole) {
        return execute(threeDimensions, threeDimensions.fastPath, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public int threeDimensionsPublisherPath(Blackhole blackhole) {
        return execute(threeDimensions, threeDimensions.publisherPath, blackhole);
    }

    private static GroupCase createCase(String columns,
                                        int dimensions,
                                        int regions,
                                        int sites,
                                        int devices) {
        String sql = "select " + columns + ",count(1) total from test group by _window(" + INPUT_ROWS
                + ")," + columns;
        Map<String, Object>[] rows = createRows(regions, sites, devices);
        int groups = regions * sites * devices;
        int rowsPerGroup = INPUT_ROWS / groups;
        return new GroupCase(dimensions,
                             rows,
                             groups,
                             rowsPerGroup,
                             ReactorQL.builder().sql(sql).build(),
                             ReactorQL.builder()
                                      .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                      .sql(sql)
                                      .build());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createRows(int regions, int sites, int devices) {
        Map<String, Object>[] rows = new Map[INPUT_ROWS];
        for (int index = 0; index < INPUT_ROWS; index++) {
            int region = index % regions;
            int site = (index / regions) % sites;
            int device = (index / (regions * sites)) % devices;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("region", "r" + region);
            row.put("site", "s" + site);
            row.put("device", "d" + device);
            rows[index] = row;
        }
        return rows;
    }

    private static void verify(GroupCase groupCase, ReactorQL query) {
        List<Map<String, Object>> results = executeQuery(groupCase, query);
        assertResult(groupCase, results);
    }

    private static int execute(GroupCase groupCase, ReactorQL query, Blackhole blackhole) {
        List<Map<String, Object>> results = query.start(Flux.fromArray(groupCase.rows)).collectList().block();
        if (results == null || results.size() != groupCase.expectedGroups) {
            throw new IllegalStateException("分组 SQL 结果数量变化");
        }
        blackhole.consume(results);
        return results.size();
    }

    private static List<Map<String, Object>> executeQuery(GroupCase groupCase, ReactorQL query) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> results = query
                .start(Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.fromArray(groupCase.rows);
                }))
                .collectList()
                .block();
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("分组 SQL 必须每次只订阅输入一次: " + subscriptions.get());
        }
        if (results == null) {
            throw new IllegalStateException("分组 SQL 未产生结果列表");
        }
        return results;
    }

    private static void assertResult(GroupCase groupCase, List<Map<String, Object>> results) {
        if (results.size() != groupCase.expectedGroups) {
            throw new IllegalStateException("分组数量不符合预期: " + results.size()
                                                    + ", expected=" + groupCase.expectedGroups);
        }
        Set<String> keys = new HashSet<>();
        long count = 0;
        for (Map<String, Object> result : results) {
            count += assertGroup(groupCase, result, keys);
        }
        if (count != INPUT_ROWS) {
            throw new IllegalStateException("分组 count 总和不符合输入行数: " + count);
        }
    }

    private static long assertGroup(GroupCase groupCase, Map<String, Object> result, Set<String> keys) {
        Object region = result.get("region");
        Object site = result.get("site");
        Object device = result.get("device");
        Object total = result.get("total");
        if (region == null || total == null
                || (groupCase.dimensions > 1 && site == null)
                || (groupCase.dimensions > 2 && device == null)) {
            throw new IllegalStateException("分组值不完整: " + result);
        }
        long groupCount = ((Number) total).longValue();
        if (groupCount != groupCase.rowsPerGroup) {
            throw new IllegalStateException("分组 count 不符合预期: " + groupCount
                                                    + ", expected=" + groupCase.rowsPerGroup);
        }
        String key = region + "|" + (groupCase.dimensions > 1 ? site : "")
                + "|" + (groupCase.dimensions > 2 ? device : "");
        if (!keys.add(key)) {
            throw new IllegalStateException("出现重复分组键: " + key);
        }
        return groupCount;
    }

    private static final class GroupCase {
        private final int dimensions;
        private final Map<String, Object>[] rows;
        private final int expectedGroups;
        private final int rowsPerGroup;
        private final ReactorQL fastPath;
        private final ReactorQL publisherPath;

        private GroupCase(int dimensions,
                          Map<String, Object>[] rows,
                          int expectedGroups,
                          int rowsPerGroup,
                          ReactorQL fastPath,
                          ReactorQL publisherPath) {
            this.dimensions = dimensions;
            this.rows = rows;
            this.expectedGroups = expectedGroups;
            this.rowsPerGroup = rowsPerGroup;
            this.fastPath = fastPath;
            this.publisherPath = publisherPath;
        }
    }
}
