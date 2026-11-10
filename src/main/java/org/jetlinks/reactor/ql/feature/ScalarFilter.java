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
package org.jetlinks.reactor.ql.feature;

import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

/**
 * 可同步完成的过滤条件。
 *
 * ReactorQL 在构建查询时识别此契约，并为整棵同步条件树生成单个 Reactor {@code filter}。
 * 未实现此契约的已有过滤 Feature 继续使用原有异步谓词路径。
 *
 * @since 1.0.21
 */
@FunctionalInterface
public interface ScalarFilter extends BiFunction<ReactorQLRecord, Object, Mono<Boolean>> {

    /**
     * 同步判断当前行是否匹配。
     *
     * @param record 当前查询记录
     * @param value  调用方传入的当前值
     * @return 是否匹配
     */
    boolean test(ReactorQLRecord record, Object value);

    @Override
    default Mono<Boolean> apply(ReactorQLRecord record, Object value) {
        return Mono.just(test(record, value));
    }
}
