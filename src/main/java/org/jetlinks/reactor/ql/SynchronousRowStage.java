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

import org.jetlinks.reactor.ql.feature.ScalarFilter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.function.Function;

/**
 * 编译后的同步行处理阶段。
 *
 * <p>将零到一行的同步过滤和转换合并到同一个 Reactor {@code handle} 边界。该阶段不保存
 * 订阅态，可安全复用于多个订阅；异步表达式和有状态操作不会进入此阶段。</p>
 */
final class SynchronousRowStage implements Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> {

    private final ScalarFilter filter;

    private final Function<ReactorQLRecord, ReactorQLRecord> mapper;

    SynchronousRowStage(ScalarFilter filter,
                        Function<ReactorQLRecord, ReactorQLRecord> mapper) {
        this.filter = Objects.requireNonNull(filter, "filter");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public Flux<ReactorQLRecord> apply(Flux<ReactorQLRecord> source) {
        return source.handle((record, sink) -> {
            if (filter.test(record, record.getRecord())) {
                sink.next(mapper.apply(record));
            }
        });
    }
}
