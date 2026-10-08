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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Mixed wire-format telemetry values; direct computation retains an independent numeric oracle. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class NumericTextWorkloadBenchmark {
    private static final int ROWS = 16_384;
    private static final String SQL = "select sequence,upper(device_id) device_id,"
            + "cast(event_time as bigint) event_time,cast(packets as integer) packets,"
            + "cast(counter as bigint) counter,cast(signal as integer) signal,"
            + "cast(temperature as double) temperature,cast(voltage as double) voltage,"
            + "cast(humidity as double) humidity,cast(energy as decimal) energy,"
            + "cast(channel as integer) channel,cast(clock_offset as bigint) clock_offset,"
            + "cast(precision_value as double) precision_value,cast(scientific as double) scientific,"
            + "cast(packets as bigint)*2 packet_rate,abs(cast(signal as integer)) signal_strength"
            + " from telemetry where cast(signal as integer) >= -90";

    private ReactorQL query;
    private ReactorQL nested;
    private ReactorQL decimalQuery;
    private Map<String, Object>[] rows;
    private int expectedCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        query = ReactorQL.builder().sql(SQL).build();
        nested = ReactorQL.builder().sql("select * from (select * from (" + SQL
                + ") normalized) forwarded").build();
        decimalQuery = ReactorQL.builder().sql("select sequence,cast(temperature as double) temperature,"
                + "cast(voltage as double) voltage,cast(humidity as double) humidity,"
                + "cast(scientific as double) scientific from telemetry").build();
        rows = new Map[ROWS];
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> row = new HashMap<>(24);
            row.put("sequence", index);
            row.put("device_id", "sensor-" + index % 100);
            row.put("event_time", Long.toString(1_704_067_200_000L + index));
            row.put("packets", Integer.toString(index % 4096));
            row.put("counter", Long.toString(100_000_000L + index));
            row.put("signal", Integer.toString(-100 + index % 60));
            row.put("temperature", (index % 50 - 10) + ".25");
            row.put("voltage", "3." + (20 + index % 40));
            row.put("humidity", (30 + index % 50) + ".5");
            row.put("energy", (100 + index) + ".125");
            row.put("channel", "00" + index % 16);
            row.put("clock_offset", "+" + index % 3600);
            row.put("precision_value", Long.toString(12_345_678_901_234_567L + index));
            row.put("scientific", (1 + index % 9) + ".25e3");
            rows[index] = row;
            if (oldNumber(row.get("signal")).intValue() >= -90) expectedCount++;
        }
        verify(query);
        verify(nested);
        verify(decimalQuery, Flux.fromArray(rows).map(NumericTextWorkloadBenchmark::nativeDecimalRow)
                .collectList().block(), ROWS);
    }

    private void verify(ReactorQL target) {
        List<Map<String, Object>> expected = nativeProjection().collectList().block();
        verify(target, expected, expectedCount);
    }

    private void verify(ReactorQL target, List<Map<String, Object>> expected, int count) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = target.start(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        if (actual == null || expected == null || !actual.equals(expected)
                || actual.size() != count || subscriptions.get() != 1) {
            throw new IllegalStateException("Numeric text values, order, count or subscriptions differ");
        }
        for (int index = 0; index < actual.size(); index++) {
            for (String key : expected.get(index).keySet()) {
                if (actual.get(index).get(key).getClass() != expected.get(index).get(key).getClass()) {
                    throw new IllegalStateException("Numeric text result type differs for " + key);
                }
            }
        }
    }

    /** Exact prior numeric rule, deliberately not delegated to the candidate CastUtils. */
    private static Number oldNumber(Object value) {
        BigDecimal number = new BigDecimal(String.valueOf(value));
        if (number.precision() >= 17) return number;
        if (number.scale() == 0) return number.longValue();
        return number.doubleValue();
    }

    private Flux<Map<String, Object>> nativeProjection() {
        return Flux.fromArray(rows).filter(row -> oldNumber(row.get("signal")).intValue() >= -90)
                .map(NumericTextWorkloadBenchmark::nativeRow);
    }

    private static Map<String, Object> nativeRow(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>(24);
        result.put("sequence", row.get("sequence"));
        result.put("device_id", String.valueOf(row.get("device_id")).toUpperCase(Locale.getDefault()));
        result.put("event_time", oldNumber(row.get("event_time")).longValue());
        result.put("packets", oldNumber(row.get("packets")).intValue());
        result.put("counter", oldNumber(row.get("counter")).longValue());
        result.put("signal", oldNumber(row.get("signal")).intValue());
        result.put("temperature", oldNumber(row.get("temperature")).doubleValue());
        result.put("voltage", oldNumber(row.get("voltage")).doubleValue());
        result.put("humidity", oldNumber(row.get("humidity")).doubleValue());
        result.put("energy", new BigDecimal(String.valueOf(row.get("energy"))));
        result.put("channel", oldNumber(row.get("channel")).intValue());
        result.put("clock_offset", oldNumber(row.get("clock_offset")).longValue());
        result.put("precision_value", oldNumber(row.get("precision_value")).doubleValue());
        result.put("scientific", oldNumber(row.get("scientific")).doubleValue());
        result.put("packet_rate", oldNumber(row.get("packets")).longValue() * 2L);
        result.put("signal_strength", Math.abs((double) oldNumber(row.get("signal")).intValue()));
        return result;
    }

    private void consume(Flux<Map<String, Object>> result, Blackhole blackhole) {
        consume(result, expectedCount, blackhole);
    }

    private void consume(Flux<Map<String, Object>> result, int expectedRows, Blackhole blackhole) {
        Long count = result.doOnNext(blackhole::consume).count().block();
        if (count == null || count != expectedRows) throw new IllegalStateException("Numeric text row count differs");
    }

    private static Map<String, Object> nativeDecimalRow(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sequence", row.get("sequence"));
        result.put("temperature", oldNumber(row.get("temperature")).doubleValue());
        result.put("voltage", oldNumber(row.get("voltage")).doubleValue());
        result.put("humidity", oldNumber(row.get("humidity")).doubleValue());
        result.put("scientific", oldNumber(row.get("scientific")).doubleValue());
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlProjection(Blackhole blackhole) { consume(query.start(Flux.fromArray(rows)), blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlNestedProjection(Blackhole blackhole) { consume(nested.start(Flux.fromArray(rows)), blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeProjection(Blackhole blackhole) { consume(nativeProjection(), blackhole); }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlDecimalControl(Blackhole blackhole) {
        consume(decimalQuery.start(Flux.fromArray(rows)), ROWS, blackhole);
    }
}
