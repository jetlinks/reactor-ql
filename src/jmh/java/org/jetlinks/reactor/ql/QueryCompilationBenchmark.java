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

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/** Measures parsing and plan construction separately from execution of a reusable plan. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Thread)
public class QueryCompilationBenchmark {

    @Param({"select", "condition", "json", "subquery"})
    public String scenario;

    private String sql;

    @Setup
    public void setup() {
        switch (scenario) {
            case "select": sql = "select id,value,label from test"; break;
            case "condition": sql = "select id from test where value > 20 and lower(label) like 'sen%'"; break;
            case "json": sql = "select payload->'device'->>'id' deviceId from test"; break;
            case "subquery": sql = "select (select count(1) from lookup l where l.id = o.id) hits from events o"; break;
            default: throw new IllegalArgumentException(scenario);
        }
        compile();
    }

    @Benchmark
    public ReactorQL compile() {
        return ReactorQL.builder().sql(sql).build();
    }
}
