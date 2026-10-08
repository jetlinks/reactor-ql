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
package org.jetlinks.reactor.ql.internal;

import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 会物化集合的聚合函数共用资源边界。
 *
 * <p>所有容器均由单个订阅串行更新，不使用并发集合。达到上限时明确终止查询，避免以
 * 静默丢弃或淘汰改变精确聚合结果。</p>
 */
public final class StatefulAggregationSupport {

    private StatefulAggregationSupport() {
    }

    public static int readLimit(ReactorQLMetadata metadata) {
        return BoundedStateSupport.readLimit(
                metadata,
                DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE,
                DefaultReactorQL.DEFAULT_AGGREGATE_MAX_COLLECTION_SIZE,
                DefaultReactorQL.HARD_MAX_AGGREGATE_COLLECTION_SIZE,
                "使用 1 到 " + DefaultReactorQL.HARD_MAX_AGGREGATE_COLLECTION_SIZE + " 之间的集合上限。",
                DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE + "=65536"
        );
    }

    public static Mono<List<Object>> collectList(Flux<Object> source, int max) {
        return source.collect(ArrayList::new, (list, value) -> {
            ensureCapacity(list.size(), max);
            list.add(value);
        });
    }

    public static Mono<Set<Object>> collectSet(Flux<Object> source, int max) {
        return source.collect(LinkedHashSet::new, (set, value) -> addDistinct(set, value, max));
    }

    public static Mono<Long> countDistinct(Flux<Object> source, int max) {
        // A count does not expose insertion order; collectSet remains ordered for value-producing aggregates.
        return source.collect(() -> new HashSet<Object>(),
                              (values, value) -> addDistinct(values, value, max))
                     .map(values -> (long) values.size());
    }

    public static Mono<Long> countUnique(Flux<Object> source, int max) {
        // A count does not expose key order; value-producing UNIQUE variants still use frequencies().
        return source.collect(() -> new HashMap<Object, Long>(),
                              (frequencies, value) -> addUnique(frequencies, value, max))
                     .map(StatefulAggregationSupport::uniqueCount);
    }

    public static Mono<List<Object>> collectUnique(Flux<Object> source, int max) {
        return frequencies(source, max).map(frequencies -> {
            List<Object> result = new ArrayList<>();
            frequencies.forEach((value, count) -> {
                if (count == 1L) {
                    result.add(value);
                }
            });
            return result;
        });
    }

    public static Flux<Object> distinctValues(Flux<Object> source, int max) {
        return Flux.defer(() -> {
            // Seen keys are never iterated; first-occurrence output order follows source signals.
            Set<Object> seen = new HashSet<>();
            return source.handle((value, sink) -> {
                if (!seen.contains(value)) {
                    ensureCapacity(seen.size(), max);
                    seen.add(value);
                    sink.next(value);
                }
            });
        });
    }

    public static Flux<Object> uniqueValues(Flux<Object> source, int max) {
        return frequencies(source, max)
                .flatMapMany(frequencies -> Flux.fromIterable(frequencies.entrySet()))
                .filter(entry -> entry.getValue() == 1L)
                .map(Map.Entry::getKey);
    }

    private static Mono<Map<Object, Long>> frequencies(Flux<Object> source, int max) {
        return source.collect(LinkedHashMap::new, (frequencies, value) -> addUnique(frequencies, value, max));
    }

    public static void addDistinct(Set<Object> values, Object value, int max) {
        if (!values.contains(value)) {
            ensureCapacity(values.size(), max);
            values.add(value);
        }
    }

    public static void addUnique(Map<Object, Long> frequencies, Object value, int max) {
        Long count = frequencies.get(value);
        if (count == null) {
            ensureCapacity(frequencies.size(), max);
            frequencies.put(value, 1L);
        } else if (count == 1L) {
            // UNIQUE 只区分恰好一次与重复，避免继续生成频次对象。
            frequencies.put(value, 2L);
        }
    }

    public static long uniqueCount(Map<Object, Long> frequencies) {
        long total = 0;
        for (Long count : frequencies.values()) {
            if (count == 1L) {
                total++;
            }
        }
        return total;
    }

    private static void ensureCapacity(int currentSize, int max) {
        BoundedStateSupport.ensureCapacity(
                DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE,
                currentSize,
                max,
                "增加窗口、缩小输入范围或在可信场景下调大受硬上限保护的配置。",
                "select collect_list(value) values from test group by _window(1000)"
        );
    }
}
