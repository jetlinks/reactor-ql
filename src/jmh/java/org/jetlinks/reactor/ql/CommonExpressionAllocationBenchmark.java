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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Shared prepared queries and independent subscriptions; inputs and query shape stay fixed across engines. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CommonExpressionAllocationBenchmark {
    private static final int ROWS = 1024;

    @Param({"scalar", "case", "coalesceFirst", "coalesceLast", "nested", "bracket", "mixed", "functions",
            "coalesceDeferredCase", "coalesceAsyncCase", "coalesceAsyncValue", "nestedAsyncCase",
            "mixedClock", "dateFormat", "dateExtract", "propertyCast", "userExample"})
    public String scenario;

    private ReactorQL query;
    private Map<String, Object>[] input;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        String projection;
        String caseExpression = "case when this.headers.deviceId is not null then this.headers.deviceId "
                + "when this.timestamp>0 then this.`output` else 'fallback' end";
        switch (scenario) {
            case "scalar": projection = "now n,this.timestamp ts,this.headers.deviceId h,this.`output` o"; break;
            case "case":
                projection = caseExpression + " a"; break;
            case "coalesceDeferredCase":
            case "coalesceAsyncCase":
            case "coalesceAsyncValue":
                projection = "coalesce(mixed_async()," + caseExpression + ") a"; break;
            case "nestedAsyncCase":
                projection = "upper(trim(coalesce(mixed_async()," + caseExpression + "))) a"; break;
            case "coalesceFirst":
                projection = "coalesce(this.`output`,this.headers.deviceId,this.missing_field,'fallback') a"; break;
            case "coalesceLast":
                projection = "coalesce(this.missing_one,this.missing_two,this.missing_three,this.`output`) a"; break;
            case "nested": projection = "upper(trim(this.`output`)) a"; break;
            case "bracket": projection = "this['output'] a"; break;
            case "mixedClock": projection = "now() n,this.timestamp ts,this.headers.deviceId h,this.`output` o"; break;
            case "dateFormat": projection = "date_format(this.timestamp,'yyyy-MM-dd HH:mm:ss','UTC') a"; break;
            case "dateExtract": projection = "year(this.timestamp) a"; break;
            case "propertyCast": projection = "this['timestamp::long'] a"; break;
            case "userExample":
                projection = "now n,this.timestamp ts,this.headers.deviceId h,"
                        + "coalesce(this.headers.deviceId,this.`output`,'fallback') c,this['output'] o"; break;
            case "mixed":
                projection = "now() n,this.timestamp ts,this.headers.deviceId h,"
                        + "coalesce(this.headers.deviceId,this.`output`,'fallback') c,this['output'] o"; break;
            case "functions":
                projection = "substring(upper(trim(this.`output`)),1,5) a,cast(this.timestamp as string) b"; break;
            default: throw new IllegalArgumentException(scenario);
        }
        boolean scheduled = !"coalesceDeferredCase".equals(scenario);
        boolean hasValue = "coalesceAsyncValue".equals(scenario);
        ValueMapFeature async = new ValueMapFeature() {
            @Override
            public String getId() { return FeatureId.ValueMap.of("mixed_async").getId(); }

            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return row -> {
                    Mono<?> source = Mono.defer(() -> Mono.justOrEmpty(
                            hasValue ? ((Map<?, ?>) row.getRecord()).get("output") : null));
                    return scheduled ? source.publishOn(Schedulers.parallel()) : source;
                };
            }
        };
        query = ReactorQL.builder().feature(async).sql("select " + projection + " from test").build();
        input = new Map[ROWS];
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            long timestamp = 1700000000000L + i;
            String output = " dev-" + i + " ";
            String header = (i & 3) != 3 ? "device-" + i : null;
            Map<String, Object> row = new HashMap<>();
            row.put("timestamp", timestamp); row.put("now", timestamp + 10000); row.put("output", output);
            row.put("headers", header == null ? Collections.emptyMap() : Collections.singletonMap("deviceId", header));
            input[i] = Collections.unmodifiableMap(row);
            Map<String, Object> result = new HashMap<>();
            switch (scenario) {
                case "scalar":
                    result.put("n", timestamp + 10000); result.put("ts", timestamp); result.put("o", output);
                    if (header != null) result.put("h", header);
                    break;
                case "mixedClock":
                    result.put("ts", timestamp); result.put("o", output);
                    if (header != null) result.put("h", header);
                    break;
                case "dateFormat":
                    result.put("a", java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            .withZone(java.time.ZoneOffset.UTC).format(java.time.Instant.ofEpochMilli(timestamp)));
                    break;
                case "dateExtract": result.put("a", 2023); break;
                case "propertyCast": result.put("a", timestamp); break;
                case "case":
                case "coalesceDeferredCase":
                case "coalesceAsyncCase": result.put("a", header == null ? output : header); break;
                case "coalesceAsyncValue": result.put("a", output); break;
                case "nestedAsyncCase":
                    result.put("a", (header == null ? output : header).trim().toUpperCase(Locale.ENGLISH)); break;
                case "coalesceFirst":
                case "coalesceLast":
                case "bracket": result.put("a", output); break;
                case "nested": result.put("a", output.trim().toUpperCase(Locale.ENGLISH)); break;
                case "userExample":
                case "mixed":
                    result.put("ts", timestamp); result.put("o", output); result.put("c", header == null ? output : header);
                    if (header != null) result.put("h", header);
                    if ("userExample".equals(scenario)) result.put("n", timestamp + 10000);
                    break;
                case "functions":
                    result.put("a", output.trim().toUpperCase(Locale.ENGLISH).substring(0, 5));
                    result.put("b", String.valueOf(timestamp));
                    break;
                default: throw new IllegalArgumentException(scenario);
            }
            expected.add(result);
        }
        long before = System.currentTimeMillis();
        List<Map<String, Object>> actual = query.start(Flux.fromArray(input)).collectList().block();
        long after = System.currentTimeMillis();
        if (actual.size() != ROWS) throw new IllegalStateException("Row count mismatch: " + scenario);
        // Asynchronous projection uses the engine's unordered flatMap. Compare every
        // output and type after matching unique values; synchronous cases retain order checks.
        if (scheduled && projection.contains("mixed_async()")) {
            expected.sort((left, right) -> left.get("a").toString().compareTo(right.get("a").toString()));
            actual.sort((left, right) -> left.get("a").toString().compareTo(right.get("a").toString()));
        }
        for (int i = 0; i < ROWS; i++) {
            if ("mixed".equals(scenario) || "mixedClock".equals(scenario)) {
                Object clock = actual.get(i).get("n");
                if (!(clock instanceof Long) || (Long) clock < before || (Long) clock > after) {
                    throw new IllegalStateException("Clock mismatch");
                }
                expected.get(i).put("n", clock);
            }
            if (!expected.get(i).equals(actual.get(i))) throw new IllegalStateException("Result mismatch: " + scenario);
            for (String key : expected.get(i).keySet()) {
                if (expected.get(i).get(key).getClass() != actual.get(i).get(key).getClass()) {
                    throw new IllegalStateException("Type mismatch: " + scenario);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long execute(Blackhole blackhole) {
        return query.start(Flux.fromArray(input)).doOnNext(blackhole::consume).count().block();
    }
}
