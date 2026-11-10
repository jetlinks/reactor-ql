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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Fully consumed compiled queries, normalized per root input row. JOIN fanout and derived
 * snapshots are included; asyncJoin completes column evaluation on a dedicated worker.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class RecordPassingQueryBenchmark {
    private static final int ROOT_ROWS = 64;
    private static final int FANOUT = 4;

    @Param({"join", "join3", "derived", "derived3", "asyncJoin", "leftReject", "rightReject"})
    public String scenario;

    private ReactorQL query;
    private Scheduler worker;
    private Function<String, Publisher<?>> sources;
    private long expectedCount;

    @Setup
    public void setup() {
        String sql;
        switch (scenario) {
            case "join": sql = "select a.id a_id,b.id b_id from a join b on 1=1"; break;
            case "join3": sql = "select a.id a_id,b.id b_id,c.id c_id from a join b on 1=1 join c on 1=1"; break;
            case "derived": sql = "select a.id a_id,b.id b_id from (select id from a) a join (select id from b) b on 1=1"; break;
            case "derived3": sql = "select d.a_id a_id,d.b_id b_id from (select a.id a_id,b.id b_id from (select id from a) a join (select id from b) b on 1=1) d"; break;
            case "asyncJoin": sql = "select a.id a_id,async_value(b.id) b_id from a join b on 1=1"; break;
            case "leftReject": sql = "select a.id a_id,b.id b_id from a left join b on 1=0"; break;
            case "rightReject": sql = "select a.id a_id,b.id b_id from a right join b on 1=0"; break;
            default: throw new IllegalArgumentException(scenario);
        }
        worker = Schedulers.newSingle("record-passing-benchmark");
        query = ReactorQL.builder().sql(sql).feature(new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                net.sf.jsqlparser.expression.Function function = (net.sf.jsqlparser.expression.Function) expression;
                Function<ReactorQLRecord, Publisher<?>> value = ValueMapFeature.createMapperNow(
                        function.getParameters().getExpressions().get(0), metadata);
                // Invocation and completion actually cross the scheduler boundary; no Flux.just
                // substitute. Both measured versions use this exact same fixture and worker.
                return record -> Mono.defer(() -> Mono.from(value.apply(record))).subscribeOn(worker);
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("async_value").getId();
            }
        }).build();
        List<Map<String, Object>> root = rows(ROOT_ROWS);
        List<Map<String, Object>> right = rows(FANOUT);
        sources = name -> "a".equals(name) ? Flux.fromIterable(root)
                : "b".equals(name) || "c".equals(name) ? Flux.fromIterable(right) : Flux.empty();
        Map<Map<String, Object>, Integer> expected = new HashMap<>();
        for (int a = 0; a < ROOT_ROWS; a++) {
            int rightRows = "leftReject".equals(scenario) ? 1 : FANOUT;
            for (int b = 0; b < rightRows; b++) {
                int thirdRows = "join3".equals(scenario) ? FANOUT : 1;
                for (int c = 0; c < thirdRows; c++) {
                    Map<String, Object> result = new HashMap<>();
                    if (!"rightReject".equals(scenario)) {
                        result.put("a_id", a);
                    }
                    if (!"leftReject".equals(scenario)) {
                        result.put("b_id", b);
                    }
                    if ("join3".equals(scenario)) {
                        result.put("c_id", c);
                    }
                    expected.merge(result, 1, Integer::sum);
                }
            }
        }
        List<Map<String, Object>> actual = query.start(sources).collectList().block();
        Map<Map<String, Object>, Integer> actualCounts = new HashMap<>();
        if (actual != null) {
            for (Map<String, Object> row : actual) {
                actualCounts.merge(row, 1, Integer::sum);
            }
        }
        if (!expected.equals(actualCounts)) {
            throw new IllegalStateException("JOIN values, multiplicity or derived snapshots changed: " + scenario);
        }
        expectedCount = actual.size();
    }

    private static List<Map<String, Object>> rows(int size) {
        List<Map<String, Object>> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            rows.add(Collections.singletonMap("id", i));
        }
        return rows;
    }

    @Benchmark
    @OperationsPerInvocation(ROOT_ROWS)
    public void execute(Blackhole blackhole) {
        Long count = query.start(sources).count().block();
        if (count == null || count != expectedCount) {
            throw new IllegalStateException("Result count changed: " + scenario);
        }
        blackhole.consume(count);
    }

    @TearDown
    public void close() {
        worker.dispose();
    }
}
