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
package org.jetlinks.reactor.ql.supports;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/** Measures compatibility preprocessing without parser and query-plan allocation. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Thread)
public class SqlPreprocessingBenchmark {

    @Param({"unchanged", "quoted", "aliases", "legacyJson"})
    public String scenario;

    private String sql;

    @Setup
    public void setup() {
        String expected;
        switch (scenario) {
            case "unchanged":
                sql = "select id as id,value as value,label as label from test";
                expected = sql;
                break;
            case "quoted":
                sql = "select 'as 时间',id as \"编号\" from test -- as 注释\n/* as 别名 */";
                expected = sql;
                break;
            case "aliases":
                sql = "select id as 编号,value as 值,label as 名称 from test";
                expected = "select id as \"编号\",value as \"值\",label as \"名称\" from test";
                break;
            case "legacyJson":
                sql = "select json_exists(payload) as present,json_value(payload,'$.id',0) as id from test";
                expected = "select \"json_exists\"(payload) as present,\"json_value\"(payload,'$.id',0) as id from test";
                break;
            default: throw new IllegalArgumentException(scenario);
        }
        if (!expected.equals(preprocess())) {
            throw new IllegalStateException("SQL preprocessing differs: " + scenario);
        }
    }

    @Benchmark
    public String preprocess() {
        return SqlParserUtils.quoteNonAsciiAliases(sql);
    }
}
