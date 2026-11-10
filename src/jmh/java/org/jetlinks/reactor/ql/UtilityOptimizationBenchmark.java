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
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared utility conversions, map membership and complete SQL projection controls.
 * Inputs vary across batched calls; setup checks results and source subscriptions outside timing.
 * Map fixtures use ordinary HashMap keys; special map semantics are covered by regression tests.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC", "-Duser.timezone=Asia/Shanghai"})
@State(Scope.Thread)
public class UtilityOptimizationBenchmark {
    private static final int ROWS = 256;
    private final Object[] decimalText = new Object[ROWS];
    private final Object[] integerText = new Object[ROWS];
    private final Object[] preciseText = new Object[ROWS];
    private final Object[] invalidText = new Object[ROWS];
    private final Object[] malformedText = new Object[ROWS];
    private final Object[] dateText = new Object[ROWS];
    private final Object[] epochText = new Object[ROWS];
    private final Object[] numbers = new Object[ROWS];
    private final Map<String, Object>[] input = new Map[ROWS];
    private final List<Object> maps8 = new ArrayList<>();
    private final List<Object> maps64 = new ArrayList<>();
    private Map<String, Object> miss;
    private Map<String, Object> hit;
    private ReactorQL mapQuery;
    private ReactorQL conversionQuery;
    private ReactorQL controlQuery;

    @Setup
    public void setup() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        for (int i = 0; i < 64; i++) {
            Map<String, Object> row = item(i);
            maps64.add(row);
            if (i < 8) maps8.add(row);
        }
        miss = item(1000);
        hit = item(63);
        if (CompareUtils.contains(maps8, miss) || CompareUtils.contains(maps64, miss)
                || !CompareUtils.contains(maps64, hit)) throw new IllegalStateException("Map oracle failed");
        Map<Object, Object> expectedMap = new HashMap<>();
        for (int i = 0; i < 8; i++) expectedMap.put(i, 1000 + i);
        if (!CastUtils.listToMap(maps8, "id", "value").equals(expectedMap)) {
            throw new IllegalStateException("listToMap oracle failed");
        }
        List<Map<String, Object>> expectedMaps = new ArrayList<>();
        List<Map<String, Object>> expectedConversions = new ArrayList<>();
        List<Map<String, Object>> expectedControl = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            decimalText[i] = (10000 + i) + ".625";
            integerText[i] = Integer.toString(10000 + i);
            preciseText[i] = Long.toString(12_345_678_901_234_567L + i);
            invalidText[i] = "missing-" + i;
            malformedText[i] = (10000 + i) + "e+";
            numbers[i] = 10000L + i;
            epochText[i] = Long.toString(1_793_053_744_123L + i);
            LocalDateTime date = LocalDateTime.of(2026, 10, 27, 12, 34, i % 60, 123_000_000);
            dateText[i] = date.format(formatter);
            if (!CastUtils.castNumber(decimalText[i]).equals(10000D + i + .625D)
                    || !CastUtils.castNumber(integerText[i]).equals(10000L + i)
                    || !CastUtils.castNumber(preciseText[i]).equals(new BigDecimal(preciseText[i].toString()))
                    || CastUtils.castNumber(invalidText[i], ignored -> null) != null
                    || CastUtils.castNumber(malformedText[i], ignored -> null) != null
                    || !CastUtils.castLocalDateTime(dateText[i]).equals(date)
                    || CastUtils.castEpochMillis(epochText[i]) != 1_793_053_744_123L + i
                    || !CastUtils.castInstant(dateText[i]).equals(date.atZone(ZoneId.systemDefault()).toInstant())) {
                throw new IllegalStateException("Conversion oracle failed: " + i);
            }
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("decimal_text", decimalText[i]);
            row.put("epoch_text", epochText[i]);
            row.put("date_text", dateText[i]);
            row.put("candidates", maps64);
            row.put("needle", i % 2 == 0 ? hit : miss);
            row.put("items", maps8);
            input[i] = row;
            Map<String, Object> mapped = new HashMap<>();
            mapped.put("id", i);
            mapped.put("found", i % 2 == 0);
            mapped.put("mapped", expectedMap);
            expectedMaps.add(mapped);
            Map<String, Object> converted = new HashMap<>();
            converted.put("id", i);
            converted.put("reading", 10000F + i + .625F);
            converted.put("epoch", 1_793_053_744_123L + i);
            converted.put("y", 2026);
            converted.put("m", 10);
            converted.put("formatted", date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            expectedConversions.add(converted);
            expectedControl.add(Collections.singletonMap("id", i));
        }
        mapQuery = ReactorQL.builder().sql("select id,contains_any(candidates,needle) found,"
                + "array_to_row(items,'id','value') mapped from test").build();
        conversionQuery = ReactorQL.builder().sql("select id,cast(decimal_text as float) reading,"
                + "cast(epoch_text as bigint) epoch,year(date_text) y,month(date_text) m,"
                + "date_format(date_text,'yyyy-MM-dd HH:mm:ss') formatted from test").build();
        controlQuery = ReactorQL.builder().sql("select id from test where id>=0").build();
        verify(mapQuery, expectedMaps);
        verify(conversionQuery, expectedConversions);
        verify(controlQuery, expectedControl);
    }

    private static Map<String, Object> item(int id) {
        Map<String, Object> result = new HashMap<>();
        result.put("id", id);
        result.put("value", 1000 + id);
        result.put("type", "sensor");
        result.put("online", true);
        return result;
    }

    private void verify(ReactorQL query, List<Map<String, Object>> expected) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = query.start(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(input);
        })).collectList().block();
        if (subscriptions.get() != 1 || !expected.equals(actual)) {
            throw new IllegalStateException("SQL results, order, count or subscriptions differ");
        }
    }

    private void consume(ReactorQL query, Blackhole bh) {
        Long count = query.start(Flux.fromArray(input)).doOnNext(bh::consume).count().block();
        if (count == null || count != ROWS) throw new IllegalStateException("SQL count differs");
    }

    @Benchmark public boolean mapMiss8() { return CompareUtils.contains(maps8, miss); }
    @Benchmark public boolean mapMiss64() { return CompareUtils.contains(maps64, miss); }
    @Benchmark public boolean mapLastHit64() { return CompareUtils.contains(maps64, hit); }
    @Benchmark public Map<Object, Object> listToMap8() { return CastUtils.listToMap(maps8, "id", "value"); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void decimalNumber(Blackhole bh) { for (Object x : decimalText) bh.consume(CastUtils.castNumber(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void integerNumber(Blackhole bh) { for (Object x : integerText) bh.consume(CastUtils.castNumber(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void preciseNumber(Blackhole bh) { for (Object x : preciseText) bh.consume(CastUtils.castNumber(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void invalidNumber(Blackhole bh) { for (Object x : invalidText) bh.consume(CastUtils.castNumber(x, ignored -> null)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void malformedNumber(Blackhole bh) { for (Object x : malformedText) bh.consume(CastUtils.castNumber(x, ignored -> null)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void dateLocal(Blackhole bh) { for (Object x : dateText) bh.consume(CastUtils.castLocalDateTime(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void dateEpoch(Blackhole bh) { for (Object x : epochText) bh.consume(CastUtils.castEpochMillis(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void dateInstant(Blackhole bh) { for (Object x : dateText) bh.consume(CastUtils.castInstant(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void numberControl(Blackhole bh) { for (Object x : numbers) bh.consume(CastUtils.castDouble(x)); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void sqlMapProjection(Blackhole bh) { consume(mapQuery, bh); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void sqlTextProjection(Blackhole bh) { consume(conversionQuery, bh); }
    @Benchmark @OperationsPerInvocation(ROWS)
    public void sqlControl(Blackhole bh) { consume(controlQuery, bh); }
}
