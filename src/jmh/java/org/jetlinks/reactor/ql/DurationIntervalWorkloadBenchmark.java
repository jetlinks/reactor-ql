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
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Normal mixed interval telemetry, time-bucket aggregation and numeric-interval controls. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class DurationIntervalWorkloadBenchmark {
    private static final int ROWS = 8192;
    private static final Object[] INTERVALS = {60_000L, "60000", "1m", "15 minutes", "PT1H"};
    private static final long[] INTERVAL_MILLIS = {60_000L, 60_000L, 60_000L, 900_000L, 3_600_000L};
    private static final String PROJECTION = "select sequence,device_type,reading,interval_value,"
            + "time_bucket(interval_value,event_time) bucket from telemetry";
    private static final String GROUPED = "select time_bucket(interval_value,event_time) bucket,device_type,"
            + "avg(reading) average,max(reading) maximum,count(1) total from telemetry"
            + " group by time_bucket(interval_value,event_time),device_type";

    private ReactorQL projection;
    private ReactorQL grouped;
    private Map<String, Object>[] mixedRows;
    private Map<String, Object>[] numericRows;
    private int mixedGroups;
    private int numericGroups;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        projection = ReactorQL.builder().sql(PROJECTION).build();
        grouped = ReactorQL.builder().sql(GROUPED).build();
        mixedRows = new Map[ROWS];
        numericRows = new Map[ROWS];
        for (int index = 0; index < ROWS; index++) {
            Object interval = INTERVALS[index % INTERVALS.length];
            Map<String, Object> row = new HashMap<>(8);
            row.put("sequence", index);
            row.put("device_type", "sensor-" + index % 3);
            row.put("reading", 20 + index % 80);
            row.put("event_time", 1_704_067_200_000L + index * 1000L);
            row.put("interval_value", interval);
            mixedRows[index] = row;
            Map<String, Object> numeric = new HashMap<>(row);
            long intervalMillis = oldDurationMillis(interval);
            if (intervalMillis != INTERVAL_MILLIS[index % INTERVAL_MILLIS.length]) {
                throw new IllegalStateException("Duration fixture unit conversion differs");
            }
            numeric.put("interval_value", intervalMillis);
            numericRows[index] = numeric;
        }
        verifyProjection(mixedRows);
        verifyProjection(numericRows);
        mixedGroups = verifyGroups(mixedRows);
        numericGroups = verifyGroups(numericRows);
    }

    private void verifyProjection(Map<String, Object>[] rows) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = projection.start(Flux.defer(() -> {
            subscriptions.incrementAndGet(); return Flux.fromArray(rows);
        })).collectList().block();
        List<Map<String, Object>> expected = nativeProjection(rows).collectList().block();
        if (subscriptions.get() != 1 || actual == null || !actual.equals(expected) || actual.size() != ROWS) {
            throw new IllegalStateException("Duration projection values, order, count or subscriptions differ");
        }
        verifyTypes(actual, expected);
    }

    private int verifyGroups(Map<String, Object>[] rows) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = grouped.start(Flux.defer(() -> {
            subscriptions.incrementAndGet(); return Flux.fromArray(rows);
        })).collectList().block();
        Map<List<Object>, Map<String, Object>> expected = nativeGroups(rows);
        if (subscriptions.get() != 1 || actual == null || actual.size() != expected.size()) {
            throw new IllegalStateException("Duration aggregate group count or subscriptions differ");
        }
        for (Map<String, Object> value : actual) {
            Map<String, Object> oracle = expected.remove(Arrays.asList(value.get("bucket"), value.get("device_type")));
            if (!value.equals(oracle)) throw new IllegalStateException("Duration aggregate values differ: " + value + " vs " + oracle);
            verifyTypes(Arrays.asList(value), Arrays.asList(oracle));
        }
        if (!expected.isEmpty()) throw new IllegalStateException("Duration groups missing");
        return actual.size();
    }

    private static void verifyTypes(List<Map<String, Object>> actual, List<Map<String, Object>> expected) {
        for (int index = 0; index < actual.size(); index++) {
            for (String key : expected.get(index).keySet()) {
                if (actual.get(index).get(key).getClass() != expected.get(index).get(key).getClass()) {
                    throw new IllegalStateException("Duration result type differs: " + key);
                }
            }
        }
    }

    /** Independent prior parsing rule, not delegated to DefaultReactorQLMetadata's candidate. */
    private static long oldDurationMillis(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        String text = String.valueOf(value).trim();
        try { return CastUtils.castNumber(text).longValue(); }
        catch (RuntimeException ignored) { }
        if (text.startsWith("P") || text.startsWith("-P")) return Duration.parse(text).toMillis();
        String normalized = text.toLowerCase(Locale.ENGLISH).replace(" ", "")
                .replace("milliseconds", "ms").replace("millisecond", "ms").replace("millis", "ms")
                .replace("minutes", "m").replace("minute", "m").replace("mins", "m").replace("min", "m")
                .replace("seconds", "s").replace("second", "s").replace("secs", "s").replace("sec", "s")
                .replace("hours", "h").replace("hour", "h").replace("hrs", "h").replace("hr", "h")
                .replace("days", "d").replace("day", "d").replace("weeks", "w").replace("week", "w");
        return CastUtils.parseDuration(normalized).toMillis();
    }

    private static LocalDateTime oldBucket(Map<String, Object> row) {
        long interval = oldDurationMillis(row.get("interval_value"));
        long epoch = CastUtils.castDate(row.get("event_time")).getTime();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epoch - Math.floorMod(epoch, interval)), ZoneId.systemDefault());
    }

    private static Flux<Map<String, Object>> nativeProjection(Map<String, Object>[] rows) {
        return Flux.fromArray(rows).map(row -> {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("sequence", row.get("sequence"));
            result.put("device_type", row.get("device_type"));
            result.put("reading", row.get("reading"));
            result.put("interval_value", row.get("interval_value"));
            result.put("bucket", oldBucket(row));
            return result;
        });
    }

    private static Map<List<Object>, Map<String, Object>> nativeGroups(Map<String, Object>[] rows) {
        Map<List<Object>, long[]> states = new HashMap<>();
        for (Map<String, Object> row : rows) {
            List<Object> key = Arrays.asList(oldBucket(row), row.get("device_type"));
            long[] state = states.computeIfAbsent(key, ignored -> new long[]{0, 0, Long.MIN_VALUE});
            long reading = ((Number) row.get("reading")).longValue();
            state[0]++;
            state[1] += reading;
            state[2] = Math.max(state[2], reading);
        }
        Map<List<Object>, Map<String, Object>> result = new HashMap<>();
        states.forEach((key, state) -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("bucket", key.get(0));
            value.put("device_type", key.get(1));
            value.put("average", (double) state[1] / state[0]);
            value.put("maximum", (int) state[2]);
            value.put("total", state[0]);
            result.put(key, value);
        });
        return result;
    }

    private static void consume(Flux<Map<String, Object>> result, int expected, Blackhole blackhole) {
        Long count = result.doOnNext(blackhole::consume).count().block();
        if (count == null || count != expected) throw new IllegalStateException("Duration workload row count differs");
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlMixedProjection(Blackhole blackhole) { consume(projection.start(Flux.fromArray(mixedRows)), ROWS, blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlMixedGrouped(Blackhole blackhole) { consume(grouped.start(Flux.fromArray(mixedRows)), mixedGroups, blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlNumericProjection(Blackhole blackhole) { consume(projection.start(Flux.fromArray(numericRows)), ROWS, blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlNumericGrouped(Blackhole blackhole) { consume(grouped.start(Flux.fromArray(numericRows)), numericGroups, blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeMixedProjection(Blackhole blackhole) { consume(nativeProjection(mixedRows), ROWS, blackhole); }
}
