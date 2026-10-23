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
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Common bounded telemetry report: calculated readings feed a grouped HAVING report and sorted top-N.
 * Batch is a finite report dimension, not an unbounded-window memory claim.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class LayeredGroupHavingBenchmark {

    private static final int INPUT_ROWS = 16_384;
    private static final int DEVICE_COUNT = 64;
    private static final int BATCH_COUNT = 8;
    private static final int TOP_N = 20;
    private static final double MIN_AVERAGE_TEMPERATURE = 33.5D;
    private static final List<String> COLUMNS = Arrays.asList(
            "batch_no", "device_id", "region", "avg_temp", "max_voltage", "total_load", "events", "health_score");
    private static final Comparator<ReportRow> REPORT_ORDER = Comparator.comparingDouble(ReportRow::healthScore)
                                                                        .reversed()
                                                                        .thenComparingInt(report -> report.key.batchNo)
                                                                        .thenComparing(report -> report.key.deviceId);
    private static final String INNER_SQL = "select event_seq,batch_no,device_id,region,"
            + "cast(temperature_text as double)+calibration adjusted_temp,"
            + "voltage*0.1 adjusted_voltage,load_value+1 adjusted_load "
            + "from telemetry where status='online'";
    private static final String GROUPED_SQL = "select batch_no,device_id,region,"
            + "avg(adjusted_temp) avg_temp,max(adjusted_voltage) max_voltage,"
            + "sum(adjusted_load) total_load,count(1) events "
            + "from (" + INNER_SQL + ") t group by batch_no,device_id,region "
            + "having events>=8 and avg_temp>=" + MIN_AVERAGE_TEMPERATURE;
    private static final String SQL = "select batch_no,device_id,region,avg_temp,max_voltage,total_load,events,"
            + "avg_temp+max_voltage health_score from (" + GROUPED_SQL + ") g "
            + "order by health_score desc,batch_no asc,device_id asc";

    private ReactorQL groupedHaving;
    private ReactorQL groupedHavingTopN;
    private Map<String, Object>[] rows;
    private int expectedRows;
    private int expectedTopRows;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        groupedHaving = ReactorQL.builder().sql(SQL).build();
        groupedHavingTopN = ReactorQL.builder().sql(SQL + " limit " + TOP_N).build();
        rows = new Map[INPUT_ROWS];
        for (int index = 0; index < INPUT_ROWS; index++) {
            int device = index % DEVICE_COUNT;
            int batch = index / (INPUT_ROWS / BATCH_COUNT);
            int round = index / DEVICE_COUNT;
            Map<String, Object> row = new LinkedHashMap<>(10);
            row.put("event_seq", index);
            row.put("batch_no", batch);
            row.put("device_id", "device-" + device);
            row.put("region", "region-" + device % 4);
            row.put("temperature_text", (22 + (round + batch * 3) % 24) + ".25");
            row.put("calibration", (device % 8 - 4) * 0.25D);
            row.put("voltage", 30D + device % 16 + round % 5 * 0.1D);
            row.put("load_value", 10 + (round + device) % 7);
            // Offline cadence is per event round, so every device key retains multiple events.
            row.put("status", round % 8 == 0 ? "offline" : "online");
            rows[index] = row;
        }

        List<Map<String, Object>> expected = oracle(false);
        List<Map<String, Object>> expectedTop = oracle(true);
        expectedRows = expected.size();
        expectedTopRows = expectedTop.size();
        if (expectedRows == 0 || expectedRows == BATCH_COUNT * DEVICE_COUNT || expectedTopRows != TOP_N) {
            throw new IllegalStateException("Layered grouped fixture does not exercise HAVING/top-N: " + expectedRows);
        }
        verifySql(groupedHaving, expected, "full");
        verifySql(groupedHavingTopN, expectedTop, "top-N");
        verifyNative(false, expected, "full");
        verifyNative(true, expectedTop, "top-N");
    }

    private void verifySql(ReactorQL query, List<Map<String, Object>> expected, String scenario) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = query.start(source(subscriptions)).collectList().block();
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("Layered grouped SQL " + scenario + " subscriptions: " + subscriptions.get());
        }
        assertRows(actual, expected, "Layered grouped SQL " + scenario);
    }

    private void verifyNative(boolean topN, List<Map<String, Object>> expected, String scenario) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = nativeResult(topN, subscriptions).collectList().block();
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("Layered grouped native " + scenario + " subscriptions: " + subscriptions.get());
        }
        assertRows(actual, expected, "Layered grouped native " + scenario);
    }

    private Flux<Map<String, Object>> source(AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    /** Setup-only independent oracle: Java arithmetic, parsing, grouping, HAVING and stable report ordering. */
    private List<Map<String, Object>> oracle(boolean topN) {
        return format(aggregateRows(), topN);
    }

    /** Native cost reference deliberately recomputes every stage from the current input on subscription. */
    private Flux<Map<String, Object>> nativeResult(boolean topN, AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            if (subscriptions != null) {
                subscriptions.incrementAndGet();
            }
            return Flux.fromIterable(format(aggregateRows(), topN));
        });
    }

    /** Plain-Java reference work shared by setup and native measurement, never cached between subscriptions. */
    private Map<GroupKey, Aggregate> aggregateRows() {
        Map<GroupKey, Aggregate> groups = new HashMap<>();
        for (Map<String, Object> row : rows) {
            if (!"online".equals(row.get("status"))) {
                continue;
            }
            double adjustedTemperature = Double.parseDouble(String.valueOf(row.get("temperature_text")))
                    + ((Number) row.get("calibration")).doubleValue();
            double adjustedVoltage = ((Number) row.get("voltage")).doubleValue() * 0.1D;
            double adjustedLoad = ((Number) row.get("load_value")).doubleValue() + 1D;
            GroupKey key = new GroupKey((Integer) row.get("batch_no"),
                                        String.valueOf(row.get("device_id")),
                                        String.valueOf(row.get("region")));
            groups.computeIfAbsent(key, ignored -> new Aggregate()).add(adjustedTemperature, adjustedVoltage, adjustedLoad);
        }
        return groups;
    }

    private static List<Map<String, Object>> format(Map<GroupKey, Aggregate> groups, boolean topN) {
        List<ReportRow> reports = new ArrayList<>();
        for (Map.Entry<GroupKey, Aggregate> entry : groups.entrySet()) {
            Aggregate aggregate = entry.getValue();
            double average = aggregate.sumTemperature / aggregate.events;
            if (aggregate.events < 8 || average < MIN_AVERAGE_TEMPERATURE) {
                continue;
            }
            reports.add(new ReportRow(entry.getKey(), average, aggregate.maxVoltage,
                                      aggregate.sumLoad, aggregate.events));
        }
        if (topN) {
            PriorityQueue<ReportRow> top = new PriorityQueue<>(TOP_N, REPORT_ORDER.reversed());
            for (ReportRow report : reports) {
                top.offer(report);
                if (top.size() > TOP_N) {
                    top.poll();
                }
            }
            reports = new ArrayList<>(top);
        }
        reports.sort(REPORT_ORDER);
        int size = reports.size();
        List<Map<String, Object>> result = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            result.add(reports.get(index).toMap());
        }
        return result;
    }

    private static void assertRows(List<Map<String, Object>> actual,
                                   List<Map<String, Object>> expected,
                                   String scenario) {
        if (actual == null || actual.size() != expected.size()) {
            throw new IllegalStateException(scenario + " row count: "
                    + (actual == null ? null : actual.size()) + ", expected=" + expected.size());
        }
        for (int index = 0; index < expected.size(); index++) {
            Map<String, Object> actualRow = actual.get(index);
            Map<String, Object> expectedRow = expected.get(index);
            if (actualRow.size() != COLUMNS.size() || !actualRow.keySet().containsAll(COLUMNS)) {
                throw new IllegalStateException(scenario + " columns at " + index + ": " + actualRow.keySet());
            }
            for (String column : COLUMNS) {
                assertValue(actualRow.get(column), expectedRow.get(column), scenario + " " + column + " at " + index);
            }
        }
    }

    private static void assertValue(Object actual, Object expected, String location) {
        if (actual == null || actual.getClass() != expected.getClass()) {
            throw new IllegalStateException(location + " type: "
                    + (actual == null ? null : actual.getClass()) + ", expected=" + expected.getClass());
        }
        if (actual instanceof Double
                && Double.doubleToLongBits((Double) actual) != Double.doubleToLongBits((Double) expected)) {
            throw new IllegalStateException(location + " double value: " + actual + ", expected=" + expected);
        }
        if (!(actual instanceof Double) && !actual.equals(expected)) {
            throw new IllegalStateException(location + " value: " + actual + ", expected=" + expected);
        }
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlLayeredGroupedHaving(Blackhole blackhole) {
        consume(groupedHaving.start(Flux.fromArray(rows)), expectedRows, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void sqlLayeredGroupedHavingTopN(Blackhole blackhole) {
        consume(groupedHavingTopN.start(Flux.fromArray(rows)), expectedTopRows, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeLayeredGroupedHaving(Blackhole blackhole) {
        consume(nativeResult(false, null), expectedRows, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(INPUT_ROWS)
    public void nativeLayeredGroupedHavingTopN(Blackhole blackhole) {
        consume(nativeResult(true, null), expectedTopRows, blackhole);
    }

    private static void consume(Flux<Map<String, Object>> result, int expected, Blackhole blackhole) {
        ResultSubscriber subscriber = result.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null) {
            throw new IllegalStateException("Layered grouped benchmark failed", subscriber.error);
        }
        if (!subscriber.complete || subscriber.count != expected) {
            throw new IllegalStateException("Layered grouped benchmark rows: " + subscriber.count
                    + ", expected=" + expected);
        }
    }

    private static final class GroupKey {
        private final int batchNo;
        private final String deviceId;
        private final String region;

        private GroupKey(int batchNo, String deviceId, String region) {
            this.batchNo = batchNo;
            this.deviceId = deviceId;
            this.region = region;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof GroupKey)) return false;
            GroupKey key = (GroupKey) other;
            return batchNo == key.batchNo && deviceId.equals(key.deviceId) && region.equals(key.region);
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(batchNo);
            result = 31 * result + deviceId.hashCode();
            return 31 * result + region.hashCode();
        }
    }

    private static final class Aggregate {
        private long events;
        private double sumTemperature;
        private double maxVoltage = Double.NEGATIVE_INFINITY;
        private double sumLoad;

        private void add(double temperature, double voltage, double load) {
            events++;
            sumTemperature += temperature;
            maxVoltage = Math.max(maxVoltage, voltage);
            sumLoad += load;
        }
    }

    private static final class ReportRow {
        private final GroupKey key;
        private final double average;
        private final double maxVoltage;
        private final double totalLoad;
        private final long events;

        private ReportRow(GroupKey key, double average, double maxVoltage, double totalLoad, long events) {
            this.key = key;
            this.average = average;
            this.maxVoltage = maxVoltage;
            this.totalLoad = totalLoad;
            this.events = events;
        }

        private double healthScore() {
            return average + maxVoltage;
        }

        private Map<String, Object> toMap() {
            Map<String, Object> result = new LinkedHashMap<>(8);
            result.put("batch_no", key.batchNo);
            result.put("device_id", key.deviceId);
            result.put("region", key.region);
            result.put("avg_temp", average);
            result.put("max_voltage", maxVoltage);
            result.put("total_load", totalLoad);
            result.put("events", events);
            result.put("health_score", healthScore());
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
