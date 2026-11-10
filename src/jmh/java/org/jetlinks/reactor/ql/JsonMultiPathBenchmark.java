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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measures multiple JSON paths within one function call on prebuilt telemetry events.
 * Text and Map inputs have identical content; results are consumed without collecting rows.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class JsonMultiPathBenchmark {

    private static final int ROWS = 16_384;
    private static final String DOCUMENT = "{\"device\":{\"id\":\"sensor-17\",\"online\":true},"
            + "\"metrics\":{\"temperature\":21.5,\"voltage\":3.3,\"signal\":-68},"
            + "\"location\":{\"site\":\"workshop\",\"longitude\":120.1},\"tags\":[\"indoor\",\"battery\"]}";
    private static final String[] PATHS = {
            "$.device.id", "$.device.online", "$.metrics.temperature", "$.metrics.voltage",
            "$.metrics.signal", "$.location.site", "$.location.longitude", "$.tags[1]"
    };

    @Param({"1", "4", "8"})
    public int pathCount;

    private ReactorQL query;
    private Map<String, Object>[] textRows;
    private Map<String, Object>[] mapRows;
    private Object expected;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        Configuration configuration = Configuration.defaultConfiguration();
        Object document = configuration.jsonProvider().parse(DOCUMENT);
        List<Object> values = new ArrayList<>(pathCount);
        StringBuilder paths = new StringBuilder();
        for (int i = 0; i < pathCount; i++) {
            paths.append(",'").append(PATHS[i]).append("'");
            values.add(JsonPath.compile(PATHS[i]).read(document, configuration));
        }
        expected = pathCount == 1 ? values.get(0) : values;
        query = ReactorQL.builder()
                         .sql("select sequence,json_extract(payload" + paths + ") values,"
                                      + "json_contains_path(payload,'all'" + paths + ") present from test")
                         .build();
        textRows = new Map[ROWS];
        mapRows = new Map[ROWS];
        for (int i = 0; i < ROWS; i++) {
            textRows[i] = row(i, DOCUMENT);
            mapRows[i] = row(i, document);
        }
        verify(textRows);
        verify(mapRows);
    }

    private static Map<String, Object> row(int sequence, Object document) {
        Map<String, Object> row = new HashMap<>(4);
        row.put("sequence", sequence);
        row.put("payload", document);
        return row;
    }

    private void verify(Map<String, Object>[] rows) {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger sequence = new AtomicInteger();
        long count = query.start(Flux.fromArray(rows)
                                     .doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                          .doOnNext(result -> {
                              Object actual = result.get("values");
                              if (!Objects.equals(expected, actual)
                                      || (pathCount == 1 ? !(actual instanceof String) : !(actual instanceof List))
                                      || !Boolean.TRUE.equals(result.get("present"))
                                      || !Objects.equals(sequence.getAndIncrement(), result.get("sequence"))
                                      || result.size() != 3) {
                                  throw new IllegalStateException("Unexpected JSON paths result: " + result);
                              }
                          })
                          .count()
                          .block();
        if (count != ROWS || sequence.get() != ROWS || subscriptions.get() != 1) {
            throw new IllegalStateException("Unexpected rows or source subscriptions");
        }
    }

    private void consume(Map<String, Object>[] rows, Blackhole blackhole) {
        long count = query.start(Flux.fromArray(rows)).doOnNext(blackhole::consume).count().block();
        if (count != ROWS) {
            throw new IllegalStateException("Unexpected row count: " + count);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void textDocument(Blackhole blackhole) {
        consume(textRows, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void mapDocument(Blackhole blackhole) {
        consume(mapRows, blackhole);
    }
}
