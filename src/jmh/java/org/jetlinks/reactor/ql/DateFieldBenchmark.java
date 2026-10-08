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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

/**
 * Measures date-field projection on prebuilt telemetry events, separately from input generation.
 * The Java control performs the same conversions per column and does not share parsed dates.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class DateFieldBenchmark {

    private static final int ROWS = 16_384;
    private static final String[] FUNCTIONS = {
            "year", "month", "day_of_month", "day_of_year", "day_of_week", "hour", "minute", "second"
    };
    private static final String[] KEYS = {
            "field_0", "field_1", "field_2", "field_3", "field_4", "field_5", "field_6", "field_7"
    };
    @SuppressWarnings("unchecked")
    private static final ToIntFunction<LocalDateTime>[] FIELDS = new ToIntFunction[]{
            (ToIntFunction<LocalDateTime>) LocalDateTime::getYear,
            (ToIntFunction<LocalDateTime>) LocalDateTime::getMonthValue,
            (ToIntFunction<LocalDateTime>) LocalDateTime::getDayOfMonth,
            (ToIntFunction<LocalDateTime>) LocalDateTime::getDayOfYear,
            (ToIntFunction<LocalDateTime>) time -> time.getDayOfWeek().getValue(),
            (ToIntFunction<LocalDateTime>) LocalDateTime::getHour,
            (ToIntFunction<LocalDateTime>) LocalDateTime::getMinute,
            (ToIntFunction<LocalDateTime>) LocalDateTime::getSecond
    };

    @Param({"1", "8"})
    public int fieldCount;

    @Param({"local", "text"})
    public String inputKind;

    private ReactorQL query;
    private Map<String, Object>[] rows;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        StringBuilder sql = new StringBuilder("select sequence,deviceId");
        for (int i = 0; i < fieldCount; i++) {
            sql.append(',').append(FUNCTIONS[i]).append("(eventTime) ").append(KEYS[i]);
        }
        query = ReactorQL.builder().sql(sql.append(" from test").toString()).build();
        rows = new Map[ROWS];
        for (int i = 0; i < ROWS; i++) {
            LocalDateTime time = LocalDateTime.of(2024 + i % 3, 1 + i % 12, 1 + i % 28,
                                                 i % 24, i % 60, (i / 60) % 60);
            Map<String, Object> row = new HashMap<>(4);
            row.put("sequence", i);
            row.put("deviceId", "sensor-" + (i & 255));
            row.put("eventTime", "local".equals(inputKind) ? time : time.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            rows[i] = row;
        }
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger sequence = new AtomicInteger();
        long count = query.start(Flux.fromArray(rows).doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                          .doOnNext(result -> assertDateRow(result, sequence.getAndIncrement()))
                          .count().block();
        if (count != ROWS || sequence.get() != ROWS || subscriptions.get() != 1) {
            throw new IllegalStateException("Unexpected rows or source subscriptions");
        }
    }

    private void assertDateRow(Map<String, Object> result, int index) {
        Map<String, Object> expected = nativeRow(rows[index]);
        if (!expected.equals(result) || !Objects.equals(index, result.get("sequence"))) {
            throw new IllegalStateException("Unexpected date projection: " + result);
        }
        for (int i = 0; i < fieldCount; i++) {
            if (result.get(KEYS[i]).getClass() != Integer.class) {
                throw new IllegalStateException("Unexpected date-field type");
            }
        }
    }

    private Map<String, Object> nativeRow(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>((fieldCount + 2) * 4 / 3 + 1);
        result.put("sequence", row.get("sequence"));
        result.put("deviceId", row.get("deviceId"));
        for (int i = 0; i < fieldCount; i++) {
            result.put(KEYS[i], FIELDS[i].applyAsInt(CastUtils.castLocalDateTime(row.get("eventTime"))));
        }
        return result;
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        long count = source.doOnNext(blackhole::consume).count().block();
        if (count != ROWS) {
            throw new IllegalStateException("Unexpected row count: " + count);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlDateFields(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeDateFields(Blackhole blackhole) {
        consume(Flux.fromArray(rows).map(this::nativeRow), blackhole);
    }
}
