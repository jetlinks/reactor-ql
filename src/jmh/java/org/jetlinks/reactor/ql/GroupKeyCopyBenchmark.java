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

import org.jetlinks.reactor.ql.feature.GroupFeature;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures the group-key copy boundary directly, without using SQL-shape-specific branches.
 *
 * <p>The one/two/three dimension inputs correspond to appending to no key, one existing key,
 * and two existing keys. The pre-existing list case represents keys written by an upstream group
 * stage. Every iteration creates an independent record so appending remains the real production
 * mutation and cannot accumulate keys across benchmark invocations.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class GroupKeyCopyBenchmark {

    private static final int APPENDS = 1_024;

    private ReactorQLContext context;
    private List<Object> twoDimensionKeys;
    private List<Object> threeDimensionKeys;
    private List<Object> preExistingCollectionKeys;
    private Object[] preExistingArrayKeys;
    private Object preExistingScalarKey;

    @Setup
    public void setup() {
        context = ReactorQLContext.ofDatasource(ignore -> Flux.empty());
        twoDimensionKeys = Arrays.<Object>asList("region");
        threeDimensionKeys = Arrays.<Object>asList("region", "site");
        preExistingCollectionKeys = Arrays.<Object>asList("upstream", "region", "site", "device");
        preExistingArrayKeys = new Object[]{"upstream", "region", "site", "device"};
        preExistingScalarKey = "upstream";
    }

    @Benchmark
    @OperationsPerInvocation(APPENDS)
    public long appendOneDimension(Blackhole blackhole) {
        return append(null, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(APPENDS)
    public long appendTwoDimensions(Blackhole blackhole) {
        return append(twoDimensionKeys, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(APPENDS)
    public long appendThreeDimensions(Blackhole blackhole) {
        return append(threeDimensionKeys, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(APPENDS)
    public long appendPreExistingCollectionKeys(Blackhole blackhole) {
        return append(preExistingCollectionKeys, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(APPENDS)
    public long appendPreExistingArrayKeys(Blackhole blackhole) {
        return append(preExistingArrayKeys, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(APPENDS)
    public long appendPreExistingScalarKey(Blackhole blackhole) {
        return append(preExistingScalarKey, blackhole);
    }

    private long append(Object existingKeys, Blackhole blackhole) {
        long size = 0;
        for (int index = 0; index < APPENDS; index++) {
            ReactorQLRecord record = ReactorQLRecord.newRecord("test", index, context);
            if (existingKeys != null) {
                record.addRecord(GroupFeature.groupByKeyContext, existingKeys);
            }
            GroupFeature.writeGroupKey(record, index);
            List<?> keys = (List<?>) record.getRecordValue(GroupFeature.groupByKeyContext);
            blackhole.consume(keys);
            size += keys.size();
        }
        return size;
    }
}
