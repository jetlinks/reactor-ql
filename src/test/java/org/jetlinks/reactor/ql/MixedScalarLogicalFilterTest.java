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
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MixedScalarLogicalFilterTest {

    @Test
    void bothOperandOrdersSubscribeAsyncEvenWhenScalarDecidesResult() {
        AtomicInteger subscriptions = new AtomicInteger();
        Mono<Boolean> async = Mono.deferContextual(context -> {
            subscriptions.incrementAndGet();
            return Mono.just(context.get("value"));
        });

        assertRows(query("id < 0 and async_bool()", async), false, true);
        assertRows(query("async_bool() and id < 0", async), false, true);
        assertRows(query("id > 0 or async_bool()", async), true, false);
        assertRows(query("async_bool() or id > 0", async), true, false);
        assertEquals(4, subscriptions.get());
    }

    @Test
    void emptyAndErrorSignalsKeepLogicalSemantics() {
        assertRows(query("id > 0 and async_bool()", Mono.empty()), false, false);
        assertRows(query("async_bool() and id > 0", Mono.empty()), false, false);
        assertRows(query("id < 0 or async_bool()", Mono.empty()), false, false);
        assertRows(query("id > 0 or async_bool()", Mono.empty()), true, false);

        Mono<Boolean> failed = Mono.error(new IllegalStateException("async failure"));
        StepVerifier.create(query("id < 0 and async_bool()", failed).start(input()))
                    .expectErrorMessage("async failure")
                    .verify();
        StepVerifier.create(query("id > 0 or async_bool()", failed).start(input()))
                    .expectErrorMessage("async failure")
                    .verify();
        StepVerifier.create(query("async_bool() and id < 0", failed).start(input()))
                    .expectErrorMessage("async failure")
                    .verify();
        StepVerifier.create(query("async_bool() or id > 0", failed).start(input()))
                    .expectErrorMessage("async failure")
                    .verify();
    }

    @Test
    void cancellingDecisiveOrStillCancelsColdAsyncSource() {
        assertCancelled("id > 0 or async_bool()");
        assertCancelled("async_bool() and id < 0");
    }

    @Test
    void nestedInAndOrKeepEmptyErrorContextAndCancellation() {
        AtomicInteger subscriptions = new AtomicInteger();
        Mono<Boolean> contextual = Mono.deferContextual(context -> {
            subscriptions.incrementAndGet();
            return Mono.just(context.get("value"));
        });
        assertRows(query("id in (1,2) and async_bool() and id > 0", contextual), true, true);
        assertRows(query("async_bool() and id in (1,2) and id > 0", contextual), true, true);
        assertRows(query("id in (1,2) or async_bool() or id < 0", contextual), true, false);
        assertRows(query("async_bool() or id in (1,2) or id < 0", contextual), true, false);
        assertEquals(4, subscriptions.get());

        assertRows(query("id in (1,2) and async_bool() and id > 0", Mono.empty()), false, false);
        assertRows(query("async_bool() and id in (1,2) and id > 0", Mono.empty()), false, false);
        assertRows(query("id in (1,2) or async_bool() or id < 0", Mono.empty()), true, false);
        assertRows(query("async_bool() or id in (1,2) or id < 0", Mono.empty()), true, false);

        Mono<Boolean> failed = Mono.error(new IllegalStateException("nested failure"));
        StepVerifier.create(query("id in (1,2) or async_bool() or id < 0", failed).start(input()))
                    .expectErrorMessage("nested failure")
                    .verify();
        assertCancelled("id in (1,2) or async_bool() or id < 0");
    }

    @Test
    void nestedMixedAndKeepsScalarAndAsyncApplyOrderWithoutShortCircuit() {
        assertMixedAndOrder("trace_a() and (async_trace() and trace_b()) and trace_c()");
        assertMixedAndOrder("((trace_a() and async_trace()) and trace_b()) and trace_c()");
    }

    @Test
    void nestedMixedOrKeepsScalarAndAsyncApplyOrderWithoutShortCircuit() {
        assertMixedOrOrder("trace_a() or (async_trace() or trace_b()) or trace_c()");
        assertMixedOrOrder("((trace_a() or async_trace()) or trace_b()) or trace_c()");
    }

    private static void assertMixedAndOrder(String condition) {
        List<String> calls = new ArrayList<>();
        ReactorQL query = ReactorQL.builder()
                                   .feature(scalarTrace("trace_a", false, calls))
                                   .feature(scalarTrace("trace_b", false, calls))
                                   .feature(scalarTrace("trace_c", true, calls))
                                   .feature(asyncTrace(calls))
                                   .sql("select id from test where " + condition)
                                   .build();
        StepVerifier.create(query.start(input()).contextWrite(Context.of("value", true)))
                    .verifyComplete();
        assertEquals(Arrays.asList("trace_a", "async.apply", "trace_b", "trace_c", "async.subscribe"), calls);
    }

    private static void assertMixedOrOrder(String condition) {
        List<String> calls = new ArrayList<>();
        ReactorQL query = ReactorQL.builder()
                                   .feature(scalarTrace("trace_a", true, calls))
                                   .feature(scalarTrace("trace_b", true, calls))
                                   .feature(scalarTrace("trace_c", false, calls))
                                   .feature(asyncTrace(calls))
                                   .sql("select id from test where " + condition)
                                   .build();
        StepVerifier.create(query.start(input()).contextWrite(Context.of("value", false)))
                    .expectNext(Collections.singletonMap("id", 1))
                    .verifyComplete();
        assertEquals(Arrays.asList("trace_a", "async.apply", "trace_b", "trace_c", "async.subscribe"), calls);
    }

    private static FilterFeature asyncTrace(List<String> calls) {
        return new FilterFeature() {
            @Override
            public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression,
                                                                                       ReactorQLMetadata metadata) {
                return (record, value) -> {
                    calls.add("async.apply");
                    return Mono.<Boolean>deferContextual(context -> Mono.just(context.get("value")))
                               .doOnSubscribe(ignore -> calls.add("async.subscribe"));
                };
            }

            @Override
            public String getId() {
                return FeatureId.Filter.of("async_trace").getId();
            }
        };
    }

    private static FilterFeature scalarTrace(String name, boolean result, List<String> calls) {
        return new FilterFeature() {
            @Override
            public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression,
                                                                                       ReactorQLMetadata metadata) {
                return (org.jetlinks.reactor.ql.feature.ScalarFilter) (record, value) -> {
                    calls.add(name);
                    return result;
                };
            }

            @Override
            public String getId() {
                return FeatureId.Filter.of(name).getId();
            }
        };
    }

    private static void assertCancelled(String condition) {
        Sinks.One<Boolean> subscribed = Sinks.one();
        AtomicBoolean cancelled = new AtomicBoolean();
        Mono<Boolean> waiting = Mono.<Boolean>never()
                                    .doOnSubscribe(ignore -> subscribed.tryEmitValue(true))
                                    .doOnCancel(() -> cancelled.set(true));
        StepVerifier.create(query(condition, waiting)
                                    .start(input())
                                    .takeUntilOther(subscribed.asMono()), 0)
                    .thenRequest(1)
                    .verifyComplete();
        assertTrue(cancelled.get());
    }

    private static void assertRows(ReactorQL query, boolean matched, boolean asyncValue) {
        StepVerifier.FirstStep<java.util.Map<String, Object>> verifier = StepVerifier.create(
                query.start(input()).contextWrite(Context.of("value", asyncValue))
        );
        if (matched) {
            verifier.expectNext(Collections.singletonMap("id", 1)).verifyComplete();
        } else {
            verifier.verifyComplete();
        }
    }

    private static Flux<java.util.Map<String, Object>> input() {
        return Flux.just(Collections.singletonMap("id", 1));
    }

    private static ReactorQL query(String condition, Mono<Boolean> async) {
        FilterFeature filter = new FilterFeature() {
            @Override
            public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression,
                                                                                       ReactorQLMetadata metadata) {
                return (record, value) -> async;
            }

            @Override
            public String getId() {
                return FeatureId.Filter.of("async_bool").getId();
            }
        };
        return ReactorQL.builder()
                        .feature(filter)
                        .sql("select id from test where " + condition)
                        .build();
    }
}
