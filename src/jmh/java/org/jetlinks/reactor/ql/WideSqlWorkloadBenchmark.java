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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.jetlinks.reactor.ql.utils.CastUtils;
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
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * 真实宽投影取证夹具：分别测量无过滤投影、选择性 WHERE 和通用函数求值成本。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class WideSqlWorkloadBenchmark {

    private static final int ROWS = 65_536;
    private static final Configuration JSON_CONFIGURATION = Configuration.defaultConfiguration();
    private static final JsonProvider JSON_PROVIDER = JSON_CONFIGURATION.jsonProvider();
    private static final JsonPath LONGITUDE_PATH = JsonPath.compile("$.point.lon");
    private static final JsonPath LEVEL_PATH = JsonPath.compile("$.meta.level");
    private static final DateTimeFormatter NEXT_DAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String WHERE = " where score >= 128 and score < 896"
            + " and str_contains(text, 'beta') and active = true";
    private static final String RAW_WHERE = " where score >= 128 and score < 896 and active = true";
    private static final List<String> FUNCTION_COLUMNS = Arrays.asList(
            "sequence", "device_id", "score", "adjusted_score", "score_bucket", "high_score",
            "upper_name", "short_name", "normalized_text", "tag", "longitude", "level",
            "next_day", "days_ahead", "squared_score", "label"
    );
    private static final List<String> CONTROL_COLUMNS = Arrays.asList(
            "sequence", "device_id", "score", "temperature", "name", "text", "json",
            "event_time", "category", "region", "status", "firmware", "site", "active",
            "battery", "signal"
    );
    private static final List<String> JSON_OPERATOR_COLUMNS = Arrays.asList(
            "sequence", "device_id", "score", "temperature", "name", "text", "category",
            "region", "status", "firmware", "site", "active", "battery", "signal",
            "longitude", "level"
    );
    private static final List<String> OPERATOR_MIX_COLUMNS = Arrays.asList(
            "sequence", "device_id", "score", "temperature", "adjusted_score", "score_level",
            "score_bucket", "heat_index", "upper_name", "normalized_text", "label",
            "battery_long", "radio_margin", "text_kind", "event_time", "region"
    );

    private ReactorQL functionProjection;
    private ReactorQL oneJsonFunctionProjection;
    private ReactorQL widthControl;
    private ReactorQL noWhereWidthControl;
    private ReactorQL jsonOperatorProjection;
    private ReactorQL rawWhereWidthControl;
    private ReactorQL operatorMixProjection;
    private ReactorQL operatorMixOrProjection;
    private ReactorQL operatorMixWithoutIn;
    private ReactorQL operatorMixDynamicIn;
    private Map<String, Object>[] rows;
    private Map<String, Object>[] parsedJsonRows;
    private int[] selectedIndexes;
    private int[] rawSelectedIndexes;
    private int[] operatorMixSelectedIndexes;
    private int[] operatorMixOrSelectedIndexes;

    @Setup
    public void setup() {
        functionProjection = ReactorQL.builder().sql(functionSql(true)).build();
        oneJsonFunctionProjection = ReactorQL.builder().sql(functionSql(false)).build();
        widthControl = ReactorQL.builder().sql(controlSql()).build();
        noWhereWidthControl = ReactorQL.builder().sql(controlSql("")).build();
        jsonOperatorProjection = ReactorQL.builder().sql(jsonOperatorSql()).build();
        rawWhereWidthControl = ReactorQL.builder().sql(controlSql(RAW_WHERE)).build();
        operatorMixProjection = ReactorQL.builder().sql(operatorMixSql("status in ('online','unknown')")).build();
        operatorMixOrProjection = ReactorQL.builder()
                                          .sql(operatorMixSql("(status in ('online','unknown') or battery > 75 or signal < -70)"))
                                          .build();
        operatorMixWithoutIn = ReactorQL.builder().sql(operatorMixSql("status = 'online'")).build();
        operatorMixDynamicIn = ReactorQL.builder().sql(operatorMixSql("status in (lower('ONLINE'),'unknown')")).build();
        rows = createRows();
        parsedJsonRows = createParsedJsonRows(rows);
        selectedIndexes = selectedIndexes(rows, WideSqlWorkloadBenchmark::matchesWhere);
        rawSelectedIndexes = selectedIndexes(rows, WideSqlWorkloadBenchmark::matchesRawWhere);
        operatorMixSelectedIndexes = selectedIndexes(rows, WideSqlWorkloadBenchmark::matchesOperatorMixWhere);
        operatorMixOrSelectedIndexes = selectedIndexes(rows, WideSqlWorkloadBenchmark::matchesOperatorMixOrWhere);

        List<Map<String, Object>> twoJsonResults = verifySetup(functionProjection, true);
        List<Map<String, Object>> parsedJsonResults = verifySetup(functionProjection, true, parsedJsonRows);
        if (!twoJsonResults.equals(parsedJsonResults)) {
            throw new IllegalStateException("JSON 字符串/预解析 Map 查询结果不等价");
        }
        assertSameValueTypes(twoJsonResults, parsedJsonResults);
        verifyNativeFunctionSetup(twoJsonResults, rows);
        verifyNativeFunctionSetup(parsedJsonResults, parsedJsonRows);
        List<Map<String, Object>> oneJsonResults = verifySetup(oneJsonFunctionProjection, true);
        if (!twoJsonResults.equals(oneJsonResults)) {
            throw new IllegalStateException("单/双 JSON 查询结果不等价");
        }
        List<Map<String, Object>> sqlControlResults = verifySetup(widthControl, false);
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> nativeControlResults = nativeWidthControl(Flux.defer(() -> {
            nativeSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (nativeSubscriptions.get() != 1 || !sqlControlResults.equals(nativeControlResults)) {
            throw new IllegalStateException("原生宽投影与 SQL 结果或源订阅不等价");
        }
        verifyNoWhereSetup();
        verifyJsonOperatorSetup();
        verifyJsonInputDrivenSetup();
        verifyRawWhereSetup();
        verifyOperatorMixSetup();
        verifyOperatorMixOrSetup();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void wideProjectionWithFunctions(Blackhole blackhole) {
        consume(functionProjection.start(input()), selectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void wideProjectionWithParsedJsonInput(Blackhole blackhole) {
        consume(functionProjection.start(Flux.fromArray(parsedJsonRows)), selectedIndexes.length, blackhole);
    }

    /** Normal-data compute floor, not a replacement for SQL error/resource/extension contracts. */
    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeWideProjectionWithFunctions(Blackhole blackhole) {
        consume(nativeFunctionProjection(input()), selectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeWideProjectionWithParsedJsonInput(Blackhole blackhole) {
        consume(nativeFunctionProjection(Flux.fromArray(parsedJsonRows)), selectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void wideProjectionOneJsonControl(Blackhole blackhole) {
        consume(oneJsonFunctionProjection.start(input()), selectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void wideProjectionWidthControl(Blackhole blackhole) {
        consume(widthControl.start(input()), selectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeWideProjectionControl(Blackhole blackhole) {
        consume(nativeWidthControl(input()), selectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void noWhereWideProjection(Blackhole blackhole) {
        consume(noWhereWidthControl.start(input()), ROWS, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeNoWhereWideProjection(Blackhole blackhole) {
        consume(nativeNoWhereWidthControl(input()), ROWS, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void jsonOperatorWideProjection(Blackhole blackhole) {
        consume(jsonOperatorProjection.start(input()), ROWS, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void jsonOperatorWideProjectionWithParsedInput(Blackhole blackhole) {
        consume(jsonOperatorProjection.start(Flux.fromArray(parsedJsonRows)), ROWS, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeJsonOperatorWideProjection(Blackhole blackhole) {
        consume(input().map(WideSqlWorkloadBenchmark::nativeJsonOperatorRow), ROWS, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeJsonOperatorWideProjectionWithParsedInput(Blackhole blackhole) {
        consume(Flux.fromArray(parsedJsonRows).map(WideSqlWorkloadBenchmark::nativeJsonOperatorRow), ROWS, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void rawWhereWideProjection(Blackhole blackhole) {
        consume(rawWhereWidthControl.start(input()), rawSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeRawWhereWideProjection(Blackhole blackhole) {
        consume(nativeRawWhereWidthControl(input()), rawSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void operatorMixWideProjection(Blackhole blackhole) {
        consume(operatorMixProjection.start(input()), operatorMixSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void operatorMixOrWideProjection(Blackhole blackhole) {
        consume(operatorMixOrProjection.start(input()), operatorMixOrSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void operatorMixWithoutIn(Blackhole blackhole) {
        consume(operatorMixWithoutIn.start(input()), operatorMixSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void operatorMixDynamicIn(Blackhole blackhole) {
        consume(operatorMixDynamicIn.start(input()), operatorMixSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeOperatorMixWideProjection(Blackhole blackhole) {
        consume(nativeOperatorMixProjection(input()), operatorMixSelectedIndexes.length, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeOperatorMixOrWideProjection(Blackhole blackhole) {
        consume(nativeOperatorMixOrProjection(input()), operatorMixOrSelectedIndexes.length, blackhole);
    }

    private void verifyNativeFunctionSetup(List<Map<String, Object>> sql, Map<String, Object>[] inputRows) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> expected = nativeFunctionProjection(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(inputRows);
        })).collectList().block();
        if (subscriptions.get() != 1 || !sql.equals(expected)) {
            throw new IllegalStateException("完整原生宽函数与 SQL 的所有结果／来源订阅不等价");
        }
        assertSameValueTypes(sql, expected);
    }

    private void verifyJsonInputDrivenSetup() {
        Map<String, Object> changed = new HashMap<>(rows[0]);
        changed.put("json", "{\"point\":{\"lon\":12.5},\"meta\":{\"level\":23}}");
        Map<String, Object> expected = nativeJsonOperatorRow(changed);
        Map<String, Object> actual = jsonOperatorProjection.start(Flux.just(changed)).single().block();
        if (!expected.equals(actual) || !"12.5".equals(expected.get("longitude"))
                || !"23".equals(expected.get("level"))) {
            throw new IllegalStateException("JSON 原生对照必须读取实际文档，不能从 sequence 反推值");
        }
    }

    private void verifyOperatorMixOrSetup() {
        AtomicInteger sqlSubscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = operatorMixOrProjection.start(Flux.defer(() -> {
            sqlSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> expected = nativeOperatorMixOrProjection(Flux.defer(() -> {
            nativeSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || sql == null || sql.size() != operatorMixOrSelectedIndexes.length
                || !sql.equals(expected)) {
            throw new IllegalStateException("混合 OR 宽投影的结果或源订阅不等价");
        }
        for (int position = 0; position < sql.size(); position++) {
            Map<String, Object> actual = sql.get(position);
            assertColumns(actual, OPERATOR_MIX_COLUMNS, "混合 OR 宽投影");
            assertNumber(actual.get("sequence"), operatorMixOrSelectedIndexes[position], "operator mix OR sequence");
            for (Map.Entry<String, Object> entry : expected.get(position).entrySet()) {
                if (actual.get(entry.getKey()) == null
                        || actual.get(entry.getKey()).getClass() != entry.getValue().getClass()) {
                    throw new IllegalStateException("混合 OR 结果类型不等价: " + entry.getKey());
                }
            }
        }
        boolean status = false;
        boolean batteryOnly = false;
        boolean signalOnly = false;
        for (int index : operatorMixOrSelectedIndexes) {
            Map<String, Object> row = rows[index];
            boolean online = "online".equals(row.get("status"));
            boolean charged = ((Number) row.get("battery")).intValue() > 75;
            boolean strongSignal = ((Number) row.get("signal")).intValue() < -70;
            status |= online;
            batteryOnly |= !online && charged && !strongSignal;
            signalOnly |= !online && !charged && strongSignal;
        }
        if (!status || !batteryOnly || !signalOnly) {
            throw new IllegalStateException("混合 OR 夹具未覆盖全部告警分支");
        }
    }

    private void verifyOperatorMixSetup() {
        AtomicInteger sqlSubscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = operatorMixProjection.start(Flux.defer(() -> {
            sqlSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> nativeResults = nativeOperatorMixProjection(Flux.defer(() -> {
            nativeSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || sql == null || sql.size() != operatorMixSelectedIndexes.length
                || !sql.equals(nativeResults)) {
            throw new IllegalStateException("混合操作符宽投影的结果或源订阅不等价");
        }
        AtomicInteger withoutInSubscriptions = new AtomicInteger();
        List<Map<String, Object>> withoutIn = operatorMixWithoutIn.start(Flux.defer(() -> {
            withoutInSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (withoutInSubscriptions.get() != 1 || !sql.equals(withoutIn)) {
            throw new IllegalStateException("IN 与等价比较条件的结果或源订阅不等价");
        }
        AtomicInteger dynamicInSubscriptions = new AtomicInteger();
        List<Map<String, Object>> dynamicIn = operatorMixDynamicIn.start(Flux.defer(() -> {
            dynamicInSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (dynamicInSubscriptions.get() != 1 || !sql.equals(dynamicIn)) {
            throw new IllegalStateException("IN 常量与动态标量参数的结果或源订阅不等价");
        }
        boolean highScore = false;
        boolean lowScore = false;
        boolean betaText = false;
        boolean otherText = false;
        boolean originalLabel = false;
        boolean fallbackLabel = false;
        for (int position = 0; position < sql.size(); position++) {
            Map<String, Object> actual = sql.get(position);
            Map<String, Object> expected = nativeResults.get(position);
            assertColumns(actual, OPERATOR_MIX_COLUMNS, "混合操作符宽投影");
            assertNumber(actual.get("sequence"), operatorMixSelectedIndexes[position], "operator mix sequence");
            for (Map.Entry<String, Object> entry : expected.entrySet()) {
                Object value = actual.get(entry.getKey());
                if (value == null || value.getClass() != entry.getValue().getClass()) {
                    throw new IllegalStateException("混合操作符结果类型不等价: " + entry.getKey());
                }
            }
            highScore |= "high".equals(actual.get("score_level"));
            lowScore |= "low".equals(actual.get("score_level"));
            betaText |= "beta".equals(actual.get("text_kind"));
            otherText |= "other".equals(actual.get("text_kind"));
            originalLabel |= String.valueOf(actual.get("label")).startsWith("optional-");
            fallbackLabel |= String.valueOf(actual.get("label")).startsWith("device-name-");
        }
        if (!highScore || !lowScore || !betaText || !otherText || !originalLabel || !fallbackLabel) {
            throw new IllegalStateException("混合操作符夹具未覆盖 CASE/coalesce 的双分支");
        }
    }

    private void verifyRawWhereSetup() {
        AtomicInteger sqlSubscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = rawWhereWidthControl.start(Flux.defer(() -> {
            sqlSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> nativeResults = nativeRawWhereWidthControl(Flux.defer(() -> {
            nativeSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || sql == null || sql.size() != rawSelectedIndexes.length
                || !sql.equals(nativeResults)) {
            throw new IllegalStateException("原始 WHERE 宽投影的结果或源订阅不等价");
        }
        for (int position = 0; position < sql.size(); position++) {
            int index = rawSelectedIndexes[position];
            assertNumber(sql.get(position).get("sequence"), index, "raw where sequence");
        }
        for (int position : samplePositions(sql.size())) {
            assertControlRow(sql.get(position), rawSelectedIndexes[position]);
        }
    }

    private void verifyNoWhereSetup() {
        AtomicInteger sqlSubscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = noWhereWidthControl.start(Flux.defer(() -> {
            sqlSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> expected = nativeNoWhereWidthControl(Flux.defer(() -> {
            nativeSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        if (sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || sql == null || expected == null || sql.size() != ROWS || !sql.equals(expected)) {
            throw new IllegalStateException("无 WHERE 宽投影的结果或源订阅不等价");
        }
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> actual = sql.get(index);
            Map<String, Object> control = expected.get(index);
            assertColumns(actual, CONTROL_COLUMNS, "无 WHERE 宽投影");
            assertNumber(actual.get("sequence"), index, "no where sequence");
            for (Map.Entry<String, Object> entry : control.entrySet()) {
                Object value = actual.get(entry.getKey());
                if (entry.getValue() == null ? value != null
                        : value == null || value.getClass() != entry.getValue().getClass()) {
                    throw new IllegalStateException("无 WHERE 宽投影结果类型不等价: " + entry.getKey());
                }
            }
        }
    }

    private void verifyJsonOperatorSetup() {
        AtomicInteger textSubscriptions = new AtomicInteger();
        List<Map<String, Object>> text = jsonOperatorProjection.start(Flux.defer(() -> {
            textSubscriptions.incrementAndGet();
            return input();
        })).collectList().block();
        AtomicInteger parsedSubscriptions = new AtomicInteger();
        List<Map<String, Object>> parsed = jsonOperatorProjection.start(Flux.defer(() -> {
            parsedSubscriptions.incrementAndGet();
            return Flux.fromArray(parsedJsonRows);
        })).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> expected = Flux.defer(() -> {
            nativeSubscriptions.incrementAndGet();
            return input();
        }).map(WideSqlWorkloadBenchmark::nativeJsonOperatorRow).collectList().block();
        if (textSubscriptions.get() != 1 || parsedSubscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || text == null || parsed == null || expected == null
                || text.size() != ROWS || parsed.size() != ROWS || expected.size() != ROWS
                || !text.equals(expected) || !parsed.equals(expected)) {
            throw new IllegalStateException("JSON 操作符宽投影结果或源订阅不等价");
        }
        assertSameValueTypes(text, parsed);
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> actual = text.get(index);
            assertColumns(actual, JSON_OPERATOR_COLUMNS, "JSON 操作符宽投影");
            assertNumber(actual.get("sequence"), index, "JSON operator sequence");
            for (Map.Entry<String, Object> entry : expected.get(index).entrySet()) {
                Object value = actual.get(entry.getKey());
                if (value == null || value.getClass() != entry.getValue().getClass()) {
                    throw new IllegalStateException("JSON 操作符宽投影结果类型不等价: " + entry.getKey());
                }
            }
        }
    }

    private List<Map<String, Object>> verifySetup(ReactorQL query, boolean functions) {
        return verifySetup(query, functions, rows);
    }

    private List<Map<String, Object>> verifySetup(ReactorQL query,
                                                  boolean functions,
                                                  Map<String, Object>[] sourceRows) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = query
                .start(Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.fromArray(sourceRows);
                }))
                .collectList()
                .block();
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("宽投影 setup 源订阅次数不正确: " + subscriptions.get());
        }
        if (result == null || result.size() != selectedIndexes.length) {
            throw new IllegalStateException("宽投影 setup 结果行数不正确: "
                                                    + (result == null ? null : result.size())
                                                    + ", expected=" + selectedIndexes.length);
        }
        for (int position : samplePositions(result.size())) {
            if (functions) {
                assertFunctionRow(result.get(position), selectedIndexes[position]);
            } else {
                assertControlRow(result.get(position), selectedIndexes[position]);
            }
        }
        for (int i = 0; i < result.size(); i++) {
            Object sequence = result.get(i).get("sequence");
            if (!(sequence instanceof Number)
                    || ((Number) sequence).intValue() != selectedIndexes[i]) {
                throw new IllegalStateException("宽投影 setup 顺序或 sequence 不正确: " + i);
            }
        }
        if (functions) {
            assertCoalesceBranches(result);
        }
        return result;
    }

    private void assertCoalesceBranches(List<Map<String, Object>> result) {
        int originalPosition = -1;
        int fallbackPosition = -1;
        for (int position = 0; position < selectedIndexes.length; position++) {
            if (rows[selectedIndexes[position]].get("optional") == null) {
                fallbackPosition = position;
            } else {
                originalPosition = position;
            }
            if (originalPosition >= 0 && fallbackPosition >= 0) {
                assertFunctionRow(result.get(originalPosition), selectedIndexes[originalPosition]);
                assertFunctionRow(result.get(fallbackPosition), selectedIndexes[fallbackPosition]);
                return;
            }
        }
        throw new IllegalStateException("宽投影 setup 未覆盖 coalesce 原值/回退分支");
    }

    private static int[] samplePositions(int size) {
        if (size < 3) {
            throw new IllegalStateException("宽投影结果过少，无法覆盖首中尾样本: " + size);
        }
        return new int[]{0, size / 2, size - 1};
    }

    private static void assertFunctionRow(Map<String, Object> result, int index) {
        assertColumns(result, FUNCTION_COLUMNS, "函数宽投影");
        Map<String, Object> input = row(index);
        assertNumber(result.get("sequence"), index, "sequence");
        assertEquals("device-" + (index & 255), result.get("device_id"), "device_id");
        assertNumber(result.get("score"), index & 1023, "score");
        assertNumber(result.get("adjusted_score"), (index & 1023) + ((index % 80) * 2), "adjusted_score");
        assertNumber(result.get("score_bucket"), (index & 1023) % 17, "score_bucket");
        assertEquals((index & 1023) >= 512, result.get("high_score"), "high_score");
        String name = (String) input.get("name");
        String text = (String) input.get("text");
        assertEquals(name.toUpperCase(), result.get("upper_name"), "upper_name");
        assertEquals(name.substring(0, 8), result.get("short_name"), "short_name");
        assertEquals(text.replace('-', '_'), result.get("normalized_text"), "normalized_text");
        assertEquals("beta", result.get("tag"), "tag");
        assertNumber(result.get("longitude"), 120 + (index % 90), "longitude");
        assertNumber(result.get("level"), index & 7, "level");
        String eventTime = (String) input.get("eventTime");
        String expectedDate = LocalDate.parse(eventTime.substring(0, 10)).plusDays(1).toString();
        assertEquals(expectedDate, result.get("next_day"), "next_day");
        assertNumber(result.get("days_ahead"), 1, "days_ahead");
        assertNumber(result.get("squared_score"), (long) (index & 1023) * (index & 1023), "squared_score");
        assertEquals(input.get("optional") == null ? name : input.get("optional"),
                     result.get("label"),
                     "label");
    }

    private static void assertControlRow(Map<String, Object> result, int index) {
        assertColumns(result, CONTROL_COLUMNS, "宽度对照");
        Map<String, Object> input = row(index);
        assertNumber(result.get("sequence"), index, "sequence");
        assertEquals(input.get("deviceId"), result.get("device_id"), "device_id");
        assertNumber(result.get("score"), index & 1023, "score");
        assertNumber(result.get("temperature"), index % 80, "temperature");
        assertEquals(input.get("name"), result.get("name"), "name");
        assertEquals(input.get("text"), result.get("text"), "text");
        assertEquals(input.get("json"), result.get("json"), "json");
        assertEquals(input.get("eventTime"), result.get("event_time"), "event_time");
        assertEquals(input.get("category"), result.get("category"), "category");
        assertEquals(input.get("region"), result.get("region"), "region");
        assertEquals(input.get("status"), result.get("status"), "status");
        assertEquals(input.get("firmware"), result.get("firmware"), "firmware");
        assertEquals(input.get("site"), result.get("site"), "site");
        assertEquals(input.get("active"), result.get("active"), "active");
        assertNumber(result.get("battery"), 20 + (index % 81), "battery");
        assertNumber(result.get("signal"), -100 + (index % 55), "signal");
    }

    private static void assertColumns(Map<String, Object> result, List<String> expected, String description) {
        if (result.size() != expected.size() || !expected.containsAll(result.keySet())) {
            throw new IllegalStateException(description + " 字段集合或列数不正确: " + result.keySet());
        }
    }

    private static void assertSameValueTypes(List<Map<String, Object>> expected,
                                             List<Map<String, Object>> actual) {
        for (int row = 0; row < expected.size(); row++) {
            for (Map.Entry<String, Object> entry : expected.get(row).entrySet()) {
                Object value = actual.get(row).get(entry.getKey());
                Object expectedValue = entry.getValue();
                if (expectedValue != null && (value == null || expectedValue.getClass() != value.getClass())) {
                    throw new IllegalStateException("JSON 输入形态导致结果类型变化: row=" + row
                            + ", column=" + entry.getKey());
                }
            }
        }
    }

    private static void assertNumber(Object actual, long expected, String column) {
        if (!(actual instanceof Number) || ((Number) actual).longValue() != expected) {
            throw new IllegalStateException(column + " 不正确: " + actual + ", expected=" + expected);
        }
    }

    private static void assertEquals(Object expected, Object actual, String column) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new IllegalStateException(column + " 不正确: " + actual + ", expected=" + expected);
        }
    }

    private Flux<Map<String, Object>> input() {
        return Flux.fromArray(rows);
    }

    private static Flux<Map<String, Object>> nativeWidthControl(Flux<Map<String, Object>> source) {
        return source.handle((row, sink) -> {
            if (matchesWhere(row)) {
                sink.next(nativeControlRow(row));
            }
        });
    }

    private static Flux<Map<String, Object>> nativeNoWhereWidthControl(Flux<Map<String, Object>> source) {
        return source.map(WideSqlWorkloadBenchmark::nativeControlRow);
    }

    private static Flux<Map<String, Object>> nativeRawWhereWidthControl(Flux<Map<String, Object>> source) {
        return source.handle((row, sink) -> {
            if (matchesRawWhere(row)) {
                sink.next(nativeControlRow(row));
            }
        });
    }

    private static Flux<Map<String, Object>> nativeOperatorMixProjection(Flux<Map<String, Object>> source) {
        return source.handle((row, sink) -> {
            if (matchesOperatorMixWhere(row)) {
                sink.next(nativeOperatorMixRow(row));
            }
        });
    }

    private static Flux<Map<String, Object>> nativeFunctionProjection(Flux<Map<String, Object>> source) {
        return source.handle((row, sink) -> {
            if (matchesWhere(row)) {
                sink.next(nativeFunctionRow(row));
            }
        });
    }

    private static Map<String, Object> nativeFunctionRow(Map<String, Object> source) {
        int score = ((Number) source.get("score")).intValue();
        int temperature = ((Number) source.get("temperature")).intValue();
        String name = (String) source.get("name");
        String text = (String) source.get("text");
        int firstSeparator = text.indexOf(',');
        int nextSeparator = text.indexOf(',', firstSeparator + 1);
        Map<String, Object> result = new HashMap<>(22);
        result.put("sequence", source.get("sequence"));
        result.put("device_id", source.get("deviceId"));
        result.put("score", source.get("score"));
        result.put("adjusted_score", (long) score + (long) temperature * 2);
        result.put("score_bucket", (long) score % 17);
        result.put("high_score", score >= 512);
        result.put("upper_name", name.toUpperCase(Locale.ENGLISH));
        result.put("short_name", name.substring(0, Math.min(name.length(), 8)));
        result.put("normalized_text", text.replace('-', '_'));
        result.put("tag", firstSeparator < 0 ? "" : text.substring(firstSeparator + 1,
                                                                   nextSeparator < 0 ? text.length() : nextSeparator));
        // Match independent SQL expressions: do not share a parsed text document across columns.
        result.put("longitude", readNativeJsonPath(source.get("json"), LONGITUDE_PATH));
        result.put("level", readNativeJsonPath(source.get("json"), LEVEL_PATH));
        LocalDateTime nextDay = CastUtils.castLocalDateTime(source.get("eventTime")).plusDays(1);
        result.put("next_day", NEXT_DAY_FORMAT.format(nextDay));
        LocalDateTime shiftedTime = CastUtils.castLocalDateTime(source.get("eventTime")).plusDays(1);
        LocalDateTime originalTime = CastUtils.castLocalDateTime(source.get("eventTime"));
        result.put("days_ahead", ChronoUnit.DAYS.between(originalTime, shiftedTime));
        result.put("squared_score", Math.round(Math.pow(score, 2)) / 1D);
        result.put("label", source.get("optional") == null ? name : source.get("optional"));
        return result;
    }

    private static Object readNativeJsonPath(Object document, JsonPath path) {
        Object parsed = document instanceof CharSequence ? JSON_PROVIDER.parse(document.toString()) : document;
        return path.read(parsed, JSON_CONFIGURATION);
    }

    private static Flux<Map<String, Object>> nativeOperatorMixOrProjection(Flux<Map<String, Object>> source) {
        return source.handle((row, sink) -> {
            if (matchesOperatorMixOrWhere(row)) {
                sink.next(nativeOperatorMixRow(row));
            }
        });
    }

    private static Map<String, Object> nativeOperatorMixRow(Map<String, Object> source) {
        int score = ((Number) source.get("score")).intValue();
        int temperature = ((Number) source.get("temperature")).intValue();
        int battery = ((Number) source.get("battery")).intValue();
        int signal = ((Number) source.get("signal")).intValue();
        String name = (String) source.get("name");
        String text = (String) source.get("text");
        Map<String, Object> result = new HashMap<>(22);
        result.put("sequence", source.get("sequence"));
        result.put("device_id", source.get("deviceId"));
        result.put("score", source.get("score"));
        result.put("temperature", source.get("temperature"));
        result.put("adjusted_score", (long) score + temperature);
        result.put("score_level", score >= 512 ? "high" : "low");
        result.put("score_bucket", (long) score % 17);
        result.put("heat_index", (long) score * temperature);
        result.put("upper_name", name.toUpperCase());
        result.put("normalized_text", text.replace('-', '_'));
        result.put("label", source.get("optional") == null ? name : source.get("optional"));
        result.put("battery_long", (long) battery);
        result.put("radio_margin", (long) battery - signal);
        result.put("text_kind", text.contains("beta") ? "beta" : "other");
        result.put("event_time", source.get("eventTime"));
        result.put("region", source.get("region"));
        return result;
    }

    private static Map<String, Object> nativeControlRow(Map<String, Object> source) {
        // 同 SQL 16 列标量投影的初始容量，结果仍是逐行独立的可变 HashMap。
        Map<String, Object> result = new HashMap<>(22);
        result.put("sequence", source.get("sequence"));
        result.put("device_id", source.get("deviceId"));
        result.put("score", source.get("score"));
        result.put("temperature", source.get("temperature"));
        result.put("name", source.get("name"));
        result.put("text", source.get("text"));
        result.put("json", source.get("json"));
        result.put("event_time", source.get("eventTime"));
        result.put("category", source.get("category"));
        result.put("region", source.get("region"));
        result.put("status", source.get("status"));
        result.put("firmware", source.get("firmware"));
        result.put("site", source.get("site"));
        result.put("active", source.get("active"));
        result.put("battery", source.get("battery"));
        result.put("signal", source.get("signal"));
        return result;
    }

    private static Map<String, Object> nativeJsonOperatorRow(Map<String, Object> source) {
        Map<String, Object> result = new HashMap<>(22);
        result.put("sequence", source.get("sequence"));
        result.put("device_id", source.get("deviceId"));
        result.put("score", source.get("score"));
        result.put("temperature", source.get("temperature"));
        result.put("name", source.get("name"));
        result.put("text", source.get("text"));
        result.put("category", source.get("category"));
        result.put("region", source.get("region"));
        result.put("status", source.get("status"));
        result.put("firmware", source.get("firmware"));
        result.put("site", source.get("site"));
        result.put("active", source.get("active"));
        result.put("battery", source.get("battery"));
        result.put("signal", source.get("signal"));
        result.put("longitude", String.valueOf(readNativeJsonPath(source.get("json"), LONGITUDE_PATH)));
        result.put("level", String.valueOf(readNativeJsonPath(source.get("json"), LEVEL_PATH)));
        return result;
    }

    private static void consume(Flux<Map<String, Object>> result, int expectedCount, Blackhole blackhole) {
        ResultSubscriber subscriber = result.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null) {
            throw new IllegalStateException("宽投影基准执行失败", subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != expectedCount) {
            throw new IllegalStateException("宽投影基准未完整输出: " + subscriber.count
                                                    + ", expected=" + expectedCount);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createRows() {
        Map<String, Object>[] result = new Map[ROWS];
        for (int index = 0; index < result.length; index++) {
            result[index] = row(index);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createParsedJsonRows(Map<String, Object>[] sourceRows) {
        JsonProvider provider = Configuration.defaultConfiguration().jsonProvider();
        Map<String, Object>[] result = new Map[sourceRows.length];
        for (int index = 0; index < sourceRows.length; index++) {
            Map<String, Object> row = new LinkedHashMap<>(sourceRows[index]);
            row.put("json", provider.parse((String) sourceRows[index].get("json")));
            result[index] = row;
        }
        return result;
    }

    private static Map<String, Object> row(int index) {
        int score = index & 1023;
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("sequence", index);
        row.put("deviceId", "device-" + (index & 255));
        row.put("score", score);
        row.put("temperature", index % 80);
        row.put("name", "device-name-" + (index & 255));
        row.put("text", index % 5 == 0 ? "alpha,gamma,zone-" + (index & 7) : "alpha,beta,zone-" + (index & 7));
        row.put("json", "{\"point\":{\"lon\":" + (120 + (index % 90))
                + "},\"meta\":{\"level\":" + (index & 7) + "}}");
        row.put("jsonLevel", index & 7);
        row.put("eventTime", "2024-02-" + String.format("%02d", (index % 28) + 1) + " 08:30:00");
        row.put("optional", index % 7 == 0 ? "optional-" + (index & 31) : null);
        row.put("category", "category-" + (index & 15));
        row.put("region", "region-" + (index & 7));
        row.put("status", index % 3 == 0 ? "offline" : "online");
        row.put("firmware", "v" + (1 + (index % 4)) + "." + (index % 10));
        row.put("site", "site-" + (index & 31));
        row.put("active", (index & 3) != 0);
        row.put("battery", 20 + (index % 81));
        row.put("signal", -100 + (index % 55));
        return row;
    }

    private static int[] selectedIndexes(Map<String, Object>[] rows,
                                         Predicate<Map<String, Object>> filter) {
        int[] indexes = new int[rows.length];
        int count = 0;
        for (int index = 0; index < rows.length; index++) {
            if (filter.test(rows[index])) {
                indexes[count++] = index;
            }
        }
        return Arrays.copyOf(indexes, count);
    }

    private static boolean matchesRawWhere(Map<String, Object> row) {
        int score = ((Number) row.get("score")).intValue();
        return score >= 128 && score < 896 && Boolean.TRUE.equals(row.get("active"));
    }

    private static boolean matchesWhere(Map<String, Object> row) {
        int score = ((Number) row.get("score")).intValue();
        return score >= 128 && score < 896
                && ((String) row.get("text")).contains("beta")
                && Boolean.TRUE.equals(row.get("active"));
    }

    private static boolean matchesOperatorMixWhere(Map<String, Object> row) {
        int score = ((Number) row.get("score")).intValue();
        return score >= 128 && score <= 895
                && ((String) row.get("name")).startsWith("device-name-1")
                && "online".equals(row.get("status"))
                && (Boolean.TRUE.equals(row.get("active"))
                || ((Number) row.get("battery")).intValue() > 75)
                && (row.get("optional") == null || score >= 700);
    }

    private static boolean matchesOperatorMixOrWhere(Map<String, Object> row) {
        int score = ((Number) row.get("score")).intValue();
        return score >= 128 && score <= 895
                && ((String) row.get("name")).startsWith("device-name-1")
                && ("online".equals(row.get("status"))
                    || ((Number) row.get("battery")).intValue() > 75
                    || ((Number) row.get("signal")).intValue() < -70)
                && (Boolean.TRUE.equals(row.get("active"))
                    || ((Number) row.get("battery")).intValue() > 75)
                && (row.get("optional") == null || score >= 700);
    }

    private static String functionSql(boolean twoJsonColumns) {
        return "select sequence,deviceId device_id,score,score + temperature * 2 adjusted_score,"
                + "score % 17 score_bucket,score >= 512 high_score,upper(name) upper_name,"
                + "substring(name,1,8) short_name,replace(text,'-','_') normalized_text,"
                + "split_part(text, ',', 2) tag,json_get(json, '$.point.lon') longitude,"
                + (twoJsonColumns ? "json_get(json, '$.meta.level')" : "jsonLevel") + " level,"
                + "date_format(date_add(eventTime, 1, 'day'), 'yyyy-MM-dd') next_day,"
                + "date_diff(date_add(eventTime, 1, 'day'), eventTime, 'day') days_ahead,"
                + "round(pow(score, 2), 0) squared_score,coalesce(optional,name) label from test"
                + WHERE;
    }

    private static String controlSql() {
        return controlSql(WHERE);
    }

    private static String jsonOperatorSql() {
        return "select sequence,deviceId device_id,score,temperature,name,text,category,region,"
                + "status,firmware,site,active,battery,signal,"
                + "json->>'$.point.lon' longitude,json->>'$.meta.level' level from test";
    }

    private static String controlSql(String where) {
        return "select sequence,deviceId device_id,score,temperature,name,text,json,eventTime event_time,"
                + "category,region,status,firmware,site,active,battery,signal from test" + where;
    }

    private static String operatorMixSql(String statusPredicate) {
        return "select sequence,deviceId device_id,score,temperature,"
                + "cast(score + temperature as long) adjusted_score,"
                + "case when score >= 512 then 'high' else 'low' end score_level,"
                + "score % 17 score_bucket,score * temperature heat_index,"
                + "upper(name) upper_name,replace(text,'-','_') normalized_text,"
                + "coalesce(optional,name) label,cast(battery as long) battery_long,"
                + "battery - signal radio_margin,"
                + "case when text like '%beta%' then 'beta' else 'other' end text_kind,"
                + "eventTime event_time,region from test "
                + "where score between 128 and 895 and name like 'device-name-1%' "
                + "and " + statusPredicate + " "
                + "and (active = true or battery > 75) "
                + "and (optional is null or score >= 700)";
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
