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

import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Internal consumption boundary for mappers that distinguish first-value and multi-value reads.
 * Implementations remain cold and subscription-local; native Mono owns demand, cancellation and
 * errors. Ordinary Feature mappers retain the existing Mono.from boundary.
 */
public interface FirstValueMapper extends Function<ReactorQLRecord, Publisher<?>> {

    /**
     * Observe at most one value and cancel the remaining source without waiting for completion.
     * Empty sources and errors before the first value retain their native signals.
     * @param record current input row
     * @return cold first-value Publisher; must not collect the multi-value source
     */
    Mono<?> first(ReactorQLRecord record);

    static Mono<Object> first(Function<ReactorQLRecord, Publisher<?>> mapper, ReactorQLRecord record) {
        return Mono.from(mapper instanceof FirstValueMapper
                ? ((FirstValueMapper) mapper).first(record)
                : mapper.apply(record));
    }
}
