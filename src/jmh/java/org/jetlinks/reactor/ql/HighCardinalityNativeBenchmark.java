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
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * 精确高基数窗口聚合与手写逐键状态的性能边界；原生组仅作固定查询上界，不是生产实现。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class HighCardinalityNativeBenchmark {

    private static final int ROWS = 50_000;
    private static final Set<String> COLUMNS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("key", "total", "sum", "avg", "max")));
    private static final Set<String> COMPUTED_COLUMNS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("normalizedKey", "total", "sum", "avg", "max")));
    private static final Set<String> COUNT_COLUMNS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("normalizedKey", "total")));

    @Param({"1", "2", "50"})
    public int valuesPerKey;

    private ReactorQL query;
    private ReactorQL functionKeyCount;
    private ReactorQL normalizedKeyCount;
    private ReactorQL functionKeyAggregates;
    private ReactorQL normalizedKeyAggregates;
    private ReactorQL subqueryFunctionKeyCount;
    private ReactorQL subqueryFunctionKeyAggregates;
    private Map<String, Object>[] rows;
    private Map<String, Object>[] computedRows;
    private int groups;

    @Setup
    public void setup() {
        if (ROWS % valuesPerKey != 0) {
            throw new IllegalStateException("输入行数必须是每键行数的整数倍");
        }
        groups = ROWS / valuesPerKey;
        query = ReactorQL.builder()
                         .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, ROWS)
                         .sql("select key,count(1) total,sum(score) sum,avg(score) avg,max(score) max "
                                      + "from test group by _window(50000),key")
                         .build();
        rows = createRows();

        AtomicInteger sqlSubscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = query.start(input(sqlSubscriptions)).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> nativeResults = nativeAggregate(input(nativeSubscriptions))
                .collectList()
                .block();
        if (sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1) {
            throw new IllegalStateException("窗口聚合源订阅次数不正确: SQL=" + sqlSubscriptions
                                                    + ", native=" + nativeSubscriptions);
        }
        Map<String, Map<String, Object>> sqlByKey = verifyResults(sql, "SQL");
        Map<String, Map<String, Object>> nativeByKey = verifyResults(nativeResults, "native");
        if (!sqlByKey.equals(nativeByKey)) {
            throw new IllegalStateException("SQL 与原生窗口聚合输出不等价");
        }
        computedRows = createComputedRows();
        functionKeyCount = computedQuery("lower(key)", false);
        normalizedKeyCount = computedQuery("normalizedKey", false);
        functionKeyAggregates = computedQuery("lower(key)", true);
        normalizedKeyAggregates = computedQuery("normalizedKey", true);
        String derivedSource = "(select lower(key) normalizedKey,score from test) n";
        subqueryFunctionKeyCount = computedQuery(derivedSource, "normalizedKey", false);
        subqueryFunctionKeyAggregates = computedQuery(derivedSource, "normalizedKey", true);
        String functionPlan = ((DefaultReactorQL) functionKeyAggregates).describeExecutionPlan();
        String normalizedPlan = ((DefaultReactorQL) normalizedKeyAggregates).describeExecutionPlan();
        if (!functionPlan.contains("STATEFUL[group,") || !normalizedPlan.contains("STATEFUL[group,")) {
            throw new IllegalStateException("函数键／规范化属性键没有进入预期计划: "
                    + functionPlan + " / " + normalizedPlan);
        }
        AtomicInteger computedNativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> computedNative = nativeFunctionKeyAggregate(
                computedInput(computedNativeSubscriptions)).collectList().block(Duration.ofSeconds(30));
        if (computedNativeSubscriptions.get() != 1 || computedNative == null || computedNative.size() != groups) {
            throw new IllegalStateException("函数键原生参考的来源订阅／组数不正确");
        }
        verifyComputedCase(functionKeyCount, computedNative, false);
        verifyComputedCase(normalizedKeyCount, computedNative, false);
        verifyComputedCase(functionKeyAggregates, computedNative, true);
        verifyComputedCase(normalizedKeyAggregates, computedNative, true);
        verifySubqueryPlan(subqueryFunctionKeyCount);
        verifySubqueryPlan(subqueryFunctionKeyAggregates);
        verifyComputedCase(subqueryFunctionKeyCount, computedNative, false);
        verifyComputedCase(subqueryFunctionKeyAggregates, computedNative, true);

        // 预计算字段故意错误；派生SQL必须从实际raw key实时计算，不能从输入规律偷算。
        Map<String, Object> changedKey = new HashMap<>(computedRows[0]);
        changedKey.put("key", "DIFFERENT-KEY");
        changedKey.put("normalizedKey", "wrong-precomputed-key");
        changedKey.put("score", 42);
        List<Map<String, Object>> changedNative = nativeFunctionKeyAggregate(Flux.just(changedKey))
                .collectList().block(Duration.ofSeconds(30));
        verifyComputedCase(subqueryFunctionKeyCount, Flux.just(changedKey), changedNative, false);
        verifyComputedCase(subqueryFunctionKeyAggregates, Flux.just(changedKey), changedNative, true);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlAggregate(Blackhole blackhole) {
        consume(query.start(input(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeAggregate(Blackhole blackhole) {
        consume(nativeAggregate(input(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlFunctionKeyCount(Blackhole blackhole) {
        consume(functionKeyCount.start(computedInput(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlNormalizedKeyCount(Blackhole blackhole) {
        consume(normalizedKeyCount.start(computedInput(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlFunctionKeyAggregates(Blackhole blackhole) {
        consume(functionKeyAggregates.start(computedInput(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlNormalizedKeyAggregates(Blackhole blackhole) {
        consume(normalizedKeyAggregates.start(computedInput(null)), blackhole);
    }

    /** Query-shape control: key calculation remains inside measured SQL, not input setup. */
    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlSubqueryFunctionKeyCount(Blackhole blackhole) {
        consume(subqueryFunctionKeyCount.start(computedInput(null)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlSubqueryFunctionKeyAggregates(Blackhole blackhole) {
        consume(subqueryFunctionKeyAggregates.start(computedInput(null)), blackhole);
    }

    /** Bounded normal-data compute floor; normalization executes inside measurement. */
    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeFunctionKeyAggregates(Blackhole blackhole) {
        consume(nativeFunctionKeyAggregate(computedInput(null)), blackhole);
    }

    private static ReactorQL computedQuery(String keyExpression, boolean multipleAggregates) {
        return computedQuery("test", keyExpression, multipleAggregates);
    }

    private static ReactorQL computedQuery(String sourceExpression,
                                          String keyExpression,
                                          boolean multipleAggregates) {
        return ReactorQL.builder()
                .sql("select normalizedKey,count(1) total"
                        + (multipleAggregates ? ",sum(score) sum,avg(score) avg,max(score) max" : "")
                        + " from " + sourceExpression + " group by _window(50000)," + keyExpression)
                .build();
    }

    private static void verifySubqueryPlan(ReactorQL candidate) {
        String plan = ((DefaultReactorQL) candidate).describeExecutionPlan();
        if (!plan.contains("STATEFUL[group,")
                || !plan.contains("ASYNC_OR_STATEFUL[projection]")) {
            throw new IllegalStateException("函数键派生查询没有进入原生分组归约计划: " + plan);
        }
    }

    private Flux<Map<String, Object>> computedInput(AtomicInteger subscriptions) {
        return subscriptions == null ? Flux.fromArray(computedRows) : Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(computedRows);
        });
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object>[] createComputedRows() {
        Map<String, Object>[] result = new Map[ROWS];
        for (int index = 0; index < result.length; index++) {
            Map<String, Object> row = new HashMap<>(rows[index]);
            String normalizedKey = (String) row.get("key");
            row.put("normalizedKey", normalizedKey);
            row.put("key", (index & 1) == 0 ? normalizedKey.toUpperCase(Locale.ENGLISH) : normalizedKey);
            result[index] = row;
        }
        return result;
    }

    private void verifyComputedCase(ReactorQL candidate,
                                    List<Map<String, Object>> nativeResults,
                                    boolean multipleAggregates) {
        verifyComputedCase(candidate, Flux.fromArray(computedRows), nativeResults, multipleAggregates);
    }

    private static void verifyComputedCase(ReactorQL candidate,
                                           Flux<Map<String, Object>> source,
                                           List<Map<String, Object>> nativeResults,
                                           boolean multipleAggregates) {
        if (nativeResults == null || nativeResults.isEmpty()) {
            throw new IllegalStateException("函数键原生参考没有产生预期结果");
        }
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = candidate.start(source.doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                .collectList().block(Duration.ofSeconds(30));
        if (subscriptions.get() != 1 || actual == null || actual.size() != nativeResults.size()) {
            throw new IllegalStateException("函数分组键 SQL 的来源订阅／组数不正确");
        }
        Map<String, Map<String, Object>> expectedByKey = expectedComputedGroups(nativeResults, multipleAggregates);
        Set<String> columns = multipleAggregates ? COMPUTED_COLUMNS : COUNT_COLUMNS;
        for (Map<String, Object> result : actual) {
            assertComputedGroup(result, expectedByKey.remove(result.get("normalizedKey")), columns);
        }
        if (!expectedByKey.isEmpty()) {
            throw new IllegalStateException("函数分组键 SQL 缺少输出组");
        }
    }

    private static Map<String, Map<String, Object>> expectedComputedGroups(List<Map<String, Object>> nativeResults,
                                                                         boolean multipleAggregates) {
        Map<String, Map<String, Object>> expectedByKey = new HashMap<>();
        for (Map<String, Object> result : nativeResults) {
            Map<String, Object> expected = result;
            if (!multipleAggregates) {
                expected = new HashMap<>(4);
                expected.put("normalizedKey", result.get("normalizedKey"));
                expected.put("total", result.get("total"));
            }
            if (expectedByKey.put((String) expected.get("normalizedKey"), expected) != null) {
                throw new IllegalStateException("原生函数键参考产生重复组");
            }
        }
        return expectedByKey;
    }

    private static void assertComputedGroup(Map<String, Object> result,
                                            Map<String, Object> expected,
                                            Set<String> columns) {
        if (!result.keySet().equals(columns) || expected == null || !result.equals(expected)) {
            throw new IllegalStateException("函数分组键 SQL 的字段／值／类型不等价: " + result);
        }
    }

    private static Flux<Map<String, Object>> nativeFunctionKeyAggregate(Flux<Map<String, Object>> source) {
        return nativeAggregate(source, row -> ((String) row.get("key")).toLowerCase(Locale.ENGLISH), "normalizedKey");
    }

    private Flux<Map<String, Object>> input(AtomicInteger subscriptions) {
        if (subscriptions == null) {
            return Flux.fromArray(rows);
        }
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    private static Flux<Map<String, Object>> nativeAggregate(Flux<Map<String, Object>> source) {
        return nativeAggregate(source, row -> (String) row.get("key"), "key");
    }

    private static Flux<Map<String, Object>> nativeAggregate(Flux<Map<String, Object>> source,
                                                            Function<Map<String, Object>, String> keyMapper,
                                                            String keyColumn) {
        // 有界窗口完成后输出；每键只保留 count/sum/max，不保存输入行或 payload。
        return source.collect(LinkedHashMap<String, NativeState>::new, (states, row) -> {
            String key = keyMapper.apply(row);
            NativeState state = states.get(key);
            if (state == null) {
                state = new NativeState();
                states.put(key, state);
            }
            state.add(((Number) row.get("score")).intValue());
        })
                     .flatMapIterable(Map::entrySet)
                     .map(entry -> entry.getValue().result(entry.getKey(), keyColumn));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object>[] createRows() {
        Map<String, Object>[] result = new Map[ROWS];
        for (int index = 0; index < result.length; index++) {
            Map<String, Object> row = new HashMap<>(4);
            row.put("key", "key-" + index / valuesPerKey);
            row.put("score", index);
            row.put("payload", new byte[1024]);
            result[index] = row;
        }
        return result;
    }

    private Map<String, Map<String, Object>> verifyResults(List<Map<String, Object>> result,
                                                            String scenario) {
        if (result == null || result.size() != groups) {
            throw new IllegalStateException(scenario + " 结果数不正确: "
                                                    + (result == null ? null : result.size()));
        }
        Map<String, Map<String, Object>> byKey = new HashMap<>(groups * 2);
        for (Map<String, Object> row : result) {
            if (row.size() != COLUMNS.size() || !row.keySet().equals(COLUMNS)) {
                throw new IllegalStateException(scenario + " 结果字段不正确: " + row.keySet());
            }
            String key = (String) row.get("key");
            if (byKey.put(key, row) != null) {
                throw new IllegalStateException(scenario + " 重复 key: " + key);
            }
        }
        for (int index = 0; index < groups; index++) {
            String key = "key-" + index;
            Map<String, Object> row = byKey.get(key);
            if (row == null) {
                throw new IllegalStateException(scenario + " 缺少 key: " + key);
            }
            double expectedSum = 0;
            for (int offset = 0; offset < valuesPerKey; offset++) {
                expectedSum += index * valuesPerKey + offset;
            }
            assertNumber(row.get("total"), valuesPerKey, scenario + " total");
            assertNumber(row.get("sum"), expectedSum, scenario + " sum");
            assertNumber(row.get("avg"), expectedSum / valuesPerKey, scenario + " avg");
            assertNumber(row.get("max"), index * valuesPerKey + valuesPerKey - 1, scenario + " max");
        }
        return byKey;
    }

    private static void assertNumber(Object actual, double expected, String column) {
        if (!(actual instanceof Number) || ((Number) actual).doubleValue() != expected) {
            throw new IllegalStateException(column + " 不正确: " + actual + ", expected=" + expected);
        }
    }

    private void consume(Flux<Map<String, Object>> result, Blackhole blackhole) {
        ResultSubscriber subscriber = result.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null) {
            throw new IllegalStateException("高基数聚合基准失败", subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != groups) {
            throw new IllegalStateException("高基数聚合基准输出不完整: " + subscriber.count
                                                    + ", expected=" + groups);
        }
    }

    private static final class NativeState {
        private long count;
        private double sum;
        private int max = Integer.MIN_VALUE;

        private void add(int score) {
            count++;
            sum += score;
            max = Math.max(max, score);
        }

        private Map<String, Object> result(String key, String keyColumn) {
            Map<String, Object> result = new HashMap<>(8);
            result.put(keyColumn, key);
            result.put("total", count);
            result.put("sum", sum);
            result.put("avg", sum / count);
            result.put("max", max);
            return result;
        }
    }

    private static final class ResultSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole;
        private int count;
        private Throwable error;
        private boolean complete;

        private ResultSubscriber(Blackhole blackhole) {
            this.blackhole = blackhole;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            count++;
            blackhole.consume(value);
        }

        @Override
        protected void hookOnComplete() {
            complete = true;
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
        }
    }
}
