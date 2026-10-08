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

import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * 查询内有界状态的通用资源保护。
 *
 * <p>容器由 {@link Flux#defer} 或聚合累加器在每次订阅时创建，不跨订阅共享。
 * 达到上限时终止查询，不使用淘汰策略改变精确 SQL 结果。</p>
 */
public final class BoundedStateSupport {

    public static final int UNBOUNDED = Integer.MAX_VALUE;

    private BoundedStateSupport() {
    }

    public static int readLimit(ReactorQLMetadata metadata,
                                String setting,
                                int defaultValue,
                                int hardMax,
                                String suggestion,
                                String example) {
        int max;
        try {
            java.util.Optional<Object> configured = metadata.getSetting(setting);
            if (!configured.isPresent()) {
                return defaultValue;
            }
            max = CastUtils.castNumber(configured.get()).intValue();
        } catch (RuntimeException error) {
            throw ReactorQLException.invalidArgument(
                    "setting[" + setting + "]必须是数字",
                    suggestion,
                    example
            );
        }
        if (max < 1 || max > hardMax) {
            throw ReactorQLException.invalidArgument(
                    "非法状态上限 setting[" + setting + "]: " + max,
                    suggestion,
                    example
            );
        }
        return max;
    }

    public static boolean isBounded(int limit) {
        return limit != UNBOUNDED;
    }

    public static String describeLimit(int limit) {
        return isBounded(limit) ? String.valueOf(limit) : "unbounded";
    }

    public static void ensureCapacity(String setting,
                                      int currentSize,
                                      int max,
                                      String suggestion,
                                      String example) {
        if (currentSize >= max) {
            throw ReactorQLException.resourceLimit(
                    "查询状态超过 setting[" + setting + "]: " + max,
                    suggestion,
                    example
            );
        }
    }

    public static <K, V> void put(Map<K, V> values,
                                  K key,
                                  V value,
                                  int max,
                                  String setting,
                                  String suggestion,
                                  String example) {
        // 未达到上限时直接写入，避免新键热路径重复计算 hash；满容量后才区分覆盖和扩容。
        if (values.size() >= max && !values.containsKey(key)) {
            ensureCapacity(setting, values.size(), max, suggestion, example);
        }
        values.put(key, value);
    }

    public static <T> Mono<Set<T>> collectSet(Flux<? extends T> source,
                                               int max,
                                               String setting,
                                               String suggestion,
                                               String example) {
        return source.collect(
                () -> (Set<T>) new HashSet<T>(),
                (values, value) -> {
                    // 未达到上限时由 add 同时完成查重和写入，避免每个新值做两次 hash 查找。
                    if (values.size() >= max) {
                        if (values.contains(value)) {
                            return;
                        }
                        ensureCapacity(setting, values.size(), max, suggestion, example);
                    }
                    values.add(value);
                }
        );
    }

    public static <T, K> Flux<T> distinct(Flux<T> source,
                                           Function<? super T, ? extends K> keySelector,
                                           int max,
                                           String setting,
                                           String suggestion,
                                           String example) {
        return Flux.defer(() -> {
            Set<K> seen = new HashSet<>();
            return source.filter(value -> {
                K key = keySelector.apply(value);
                if (seen.size() >= max) {
                    if (seen.contains(key)) {
                        return false;
                    }
                    ensureCapacity(setting, seen.size(), max, suggestion, example);
                }
                return seen.add(key);
            });
        });
    }
}
