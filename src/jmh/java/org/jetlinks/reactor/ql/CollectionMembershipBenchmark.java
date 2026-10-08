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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Realistic tag membership projection with empty candidates and both matching and missing tags.
 * Input preparation and complete result verification are outside the timed methods. The direct
 * Java reference handles this String-tag schema, not the full SQL mixed-type and reactive contract.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class CollectionMembershipBenchmark {

    private static final int ROWS = 16_384;

    private List<Map<String, Object>> rows;
    private ReactorQL membershipQuery;
    private ReactorQL propertyQuery;

    @Setup
    public void setup() {
        membershipQuery = ReactorQL.builder()
                .sql("select sequence,deviceId,"
                             + "contains_all(tags,requiredTag,requiredTags) all_required,"
                             + "contains_any(tags,alerts) alerted,"
                             + "not_contains(tags,'disabled') enabled from test")
                .build();
        propertyQuery = ReactorQL.builder()
                .sql("select sequence,deviceId,requiredTag all_required,site alerted,active enabled from test")
                .build();
        rows = new ArrayList<>(ROWS);
        for (int index = 0; index < ROWS; index++) {
            List<String> tags = new ArrayList<>();
            for (int tag = 0; tag < 2 + index % 7; tag++) {
                tags.add("tag-" + tag);
            }
            if (index % 6 == 0) {
                tags.add("disabled");
            }
            Map<String, Object> row = new HashMap<>();
            row.put("sequence", index);
            row.put("deviceId", "sensor-" + (index & 255));
            row.put("tags", tags);
            row.put("requiredTag", "tag-" + index % 11);
            row.put("requiredTags", index % 4 == 0 ? Collections.emptyList()
                    : Arrays.asList("tag-" + index % 7, "tag-" + (index + 3) % 10));
            row.put("alerts", index % 5 == 0 ? Collections.emptyList()
                    : Arrays.asList("tag-" + (index + 2) % 11, "fault-" + index % 3));
            row.put("site", "workshop-" + (index & 15));
            row.put("active", (index & 1) == 0);
            rows.add(row);
        }
        verify(membershipQuery::start, this::nativeRow);
        verify(source -> source.map(this::nativeRow), this::nativeRow);
        verify(propertyQuery::start, this::propertyRow);
    }

    private Map<String, Object> nativeRow(Map<String, Object> row) {
        Collection<?> tags = (Collection<?>) row.get("tags");
        Collection<?> required = (Collection<?>) row.get("requiredTags");
        Collection<?> alerts = (Collection<?>) row.get("alerts");
        boolean alerted = false;
        for (Object alert : alerts) {
            if (tags.contains(alert)) {
                alerted = true;
                break;
            }
        }
        Map<String, Object> result = resultRow(row);
        result.put("all_required", tags.contains(row.get("requiredTag")) && tags.containsAll(required));
        result.put("alerted", alerted);
        result.put("enabled", !tags.contains("disabled"));
        return result;
    }

    private Map<String, Object> propertyRow(Map<String, Object> row) {
        Map<String, Object> result = resultRow(row);
        result.put("all_required", row.get("requiredTag"));
        result.put("alerted", row.get("site"));
        result.put("enabled", row.get("active"));
        return result;
    }

    private static Map<String, Object> resultRow(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>(7);
        result.put("sequence", row.get("sequence"));
        result.put("deviceId", row.get("deviceId"));
        return result;
    }

    private void verify(Function<Flux<Map<String, Object>>, Flux<Map<String, Object>>> runner,
                        Function<Map<String, Object>, Map<String, Object>> expectedMapper) {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger sequence = new AtomicInteger();
        long count = runner.apply(Flux.fromIterable(rows)
                                      .doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                .doOnNext(result -> {
                    int index = sequence.getAndIncrement();
                    Map<String, Object> expected = expectedMapper.apply(rows.get(index));
                    if (!expected.equals(result)) {
                        throw new IllegalStateException("Unexpected membership projection at " + index + ": " + result);
                    }
                    for (Map.Entry<String, Object> entry : expected.entrySet()) {
                        if (entry.getValue().getClass() != result.get(entry.getKey()).getClass()) {
                            throw new IllegalStateException("Unexpected projection type: " + entry.getKey());
                        }
                    }
                }).count().block();
        if (count != ROWS || sequence.get() != ROWS || subscriptions.get() != 1) {
            throw new IllegalStateException("Unexpected row count, order or source subscriptions");
        }
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        long count = source.doOnNext(blackhole::consume).count().block();
        if (count != ROWS) {
            throw new IllegalStateException("Unexpected row count: " + count);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlMembership(Blackhole blackhole) {
        consume(membershipQuery.start(Flux.fromIterable(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeMembership(Blackhole blackhole) {
        consume(Flux.fromIterable(rows).map(this::nativeRow), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlPropertyProjectionControl(Blackhole blackhole) {
        consume(propertyQuery.start(Flux.fromIterable(rows)), blackhole);
    }
}
