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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * 文本清洗的真实 SQL 与同语义 Java 正则对照；输入对象在测量前构造。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class StringTrimBenchmark {

    private static final int ROWS = 65_536;
    private static final String[] WHITESPACE = {
            "", " ", "\t", "\n", "\u000B", "\f", "\r", " \t", "\u00A0", "\u2003"
    };
    private static final Pattern LEADING = Pattern.compile("^\\s+");
    private static final Pattern TRAILING = Pattern.compile("\\s+$");

    private ReactorQL query;
    private Map<String, Object>[] rows;

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() {
        query = ReactorQL.builder()
                         .sql("select id,ltrim(text) left_text,rtrim(text) right_text,"
                                  + "upper(name) normalized,length(text) text_length from test")
                         .build();
        rows = new Map[ROWS];
        for (int i = 0; i < rows.length; i++) {
            Map<String, Object> row = new HashMap<>(4);
            row.put("id", i);
            row.put("name", "device-" + (i & 255));
            row.put("text", WHITESPACE[i % WHITESPACE.length]
                    + "sensor-" + (i & 255) + " beta "
                    + WHITESPACE[(i / WHITESPACE.length) % WHITESPACE.length]);
            rows[i] = row;
        }

        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> sql = query
                .start(Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.fromArray(rows);
                }))
                .collectList()
                .block();
        List<Map<String, Object>> nativeRows = nativeProjection(Flux.fromArray(rows)).collectList().block();
        if (subscriptions.get() != 1 || sql == null || nativeRows == null || !sql.equals(nativeRows)) {
            throw new IllegalStateException("文本清洗 SQL 与 Java 正则结果或源订阅不等价");
        }
        List<Map<String, Object>> preparedRows = nativePreparedProjection(Flux.fromArray(rows)).collectList().block();
        if (!sql.equals(preparedRows)) {
            throw new IllegalStateException("文本清洗 SQL 与预编译 Java 正则结果不等价");
        }
        for (int i = 0; i < ROWS; i++) {
            if (sql.get(i).get("text_length").getClass() != nativeRows.get(i).get("text_length").getClass()) {
                throw new IllegalStateException("文本长度类型不等价");
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlTrimProjection(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeTrimProjection(Blackhole blackhole) {
        consume(nativeProjection(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativePreparedTrimProjection(Blackhole blackhole) {
        consume(nativePreparedProjection(Flux.fromArray(rows)), blackhole);
    }

    private static Flux<Map<String, Object>> nativeProjection(Flux<Map<String, Object>> source) {
        return source.map(row -> {
            String text = (String) row.get("text");
            Map<String, Object> result = new HashMap<>(7);
            result.put("id", row.get("id"));
            result.put("left_text", text.replaceAll("^\\s+", ""));
            result.put("right_text", text.replaceAll("\\s+$", ""));
            result.put("normalized", ((String) row.get("name")).toUpperCase(Locale.ENGLISH));
            result.put("text_length", text.length());
            return result;
        });
    }

    private static Flux<Map<String, Object>> nativePreparedProjection(Flux<Map<String, Object>> source) {
        return source.map(row -> {
            String text = (String) row.get("text");
            Map<String, Object> result = new HashMap<>(7);
            result.put("id", row.get("id"));
            result.put("left_text", LEADING.matcher(text).replaceAll(""));
            result.put("right_text", TRAILING.matcher(text).replaceAll(""));
            result.put("normalized", ((String) row.get("name")).toUpperCase(Locale.ENGLISH));
            result.put("text_length", text.length());
            return result;
        });
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        ResultSubscriber subscriber = source.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null || !subscriber.complete || subscriber.count != ROWS) {
            throw new IllegalStateException("文本清洗基准执行失败: " + subscriber.count, subscriber.error);
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
