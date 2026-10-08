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
package org.jetlinks.reactor.ql.supports.filter;

import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

/**
 * Internal capability for a predicate whose Publisher emits one Boolean on normal completion.
 * It may still be asynchronous, fail, never complete or be cancelled; it is not a ScalarFilter.
 */
@FunctionalInterface
interface TotalBooleanPredicate extends BiFunction<ReactorQLRecord, Object, Mono<Boolean>> {

    static boolean isTotal(BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate) {
        return predicate instanceof TotalBooleanPredicate;
    }

    static Mono<Boolean> defaultFalseIfNeeded(
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate,
            Mono<Boolean> result) {
        return isTotal(predicate) ? result : result.defaultIfEmpty(false);
    }
}
