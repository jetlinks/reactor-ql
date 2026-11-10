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
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 普通日期字符串的 SQL 格式化与相同日期转换的 Java 对照。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class DateStringCastBenchmark {

    private static final int ROWS = 65_536;
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private ReactorQL query;
    private Map<String, Object>[] rows;
    private ZoneId zone;

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() {
        zone = ZoneId.systemDefault();
        query = ReactorQL.builder()
                         .sql("select id,date_format(eventTime,'yyyy-MM-dd HH:mm:ss') formatted,"
                                  + "eventTime source_time from test")
                         .build();
        rows = new Map[ROWS];
        for (int i = 0; i < rows.length; i++) {
            Map<String, Object> row = new HashMap<>(4);
            row.put("id", i);
            row.put("eventTime", "2024-02-" + String.format(Locale.ROOT, "%02d", (i % 28) + 1) + " 08:30:00");
            rows[i] = row;
        }

        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = query.start(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        List<Map<String, Object>> nativeRows = nativeProjection(Flux.fromArray(rows)).collectList().block();
        if (subscriptions.get() != 1 || sql == null || nativeRows == null || !sql.equals(nativeRows)) {
            throw new IllegalStateException("日期字符串 SQL 与 Java 对照结果或源订阅不等价");
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlDateFormat(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeDateFormat(Blackhole blackhole) {
        consume(nativeProjection(Flux.fromArray(rows)), blackhole);
    }

    private Flux<Map<String, Object>> nativeProjection(Flux<Map<String, Object>> source) {
        return source.map(row -> {
            Object date = row.get("eventTime");
            Map<String, Object> result = new HashMap<>(4);
            result.put("id", row.get("id"));
            result.put("formatted", FORMAT.format(CastUtils.castDate(date).toInstant().atZone(zone)));
            result.put("source_time", date);
            return result;
        });
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        ResultSubscriber subscriber = source.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null || !subscriber.complete || subscriber.count != ROWS) {
            throw new IllegalStateException("日期字符串基准执行失败: " + subscriber.count, subscriber.error);
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
        protected void hookOnNext(Map<String, Object> value) {
            blackhole.consume(value);
            count++;
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
