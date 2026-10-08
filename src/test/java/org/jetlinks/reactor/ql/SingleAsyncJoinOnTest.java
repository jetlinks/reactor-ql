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
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleAsyncJoinOnTest {

    private static final String INNER_SQL =
            "select t1.id left_id,t2.id right_id from t1 join t2 on t1.id = cold_on(t2.id)";

    @Test
    void shouldKeepValueEmptyAndSubscriptionSemantics() {
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .sql("select t1.id left_id,t2.id right_id from t1 join t2 on cold_on(t2.id)")
                .feature(coldOn(subscriptions, (value, context) -> {
                    int id = ((Number) value).intValue();
                    return id == 3 ? Mono.empty() : Mono.just(id == 1);
                }))
                .build();

        List<Map<String, Object>> rows = query.start(sources(1, 1, 2, 3)).collectList().block();
        assertEquals(2, rows.size());
        assertTrue(rows.contains(result(1, 1)));
        // all(empty) is true in the existing single-ON path.
        assertTrue(rows.contains(result(1, 3)));
        assertEquals(3, subscriptions.get());
    }

    @Test
    void shouldKeepLeftAndRightFallback() {
        ValueMapFeature feature = coldOn(new AtomicInteger(), (value, context) -> Mono.just(value));
        ReactorQL left = ReactorQL.builder()
                .sql("select t1.id left_id,t2.id right_id from t1 left join t2 on t1.id = cold_on(t2.id)")
                .feature(feature)
                .build();
        ReactorQL right = ReactorQL.builder()
                .sql("select t1.id left_id,t2.id right_id from t1 right join t2 on t1.id = cold_on(t2.id)")
                .feature(feature)
                .build();

        assertEquals(Collections.singletonList(result(1, null)), left.start(sources(1, 2)).collectList().block());
        assertEquals(Collections.singletonList(result(null, 2)), right.start(sources(1, 2)).collectList().block());
    }

    @Test
    void shouldPropagateContextErrorAndCancellation() {
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL contextQuery = ReactorQL.builder()
                .sql(INNER_SQL)
                .feature(coldOn(subscriptions, (value, context) -> {
                    assertEquals("join-context", context.get("marker"));
                    return Mono.just(value);
                }))
                .build();
        StepVerifier.create(contextQuery.start(sources(1, 1))
                                        .contextWrite(Context.of("marker", "join-context")))
                    .expectNext(result(1, 1))
                    .verifyComplete();
        assertEquals(1, subscriptions.get());

        ReactorQL errorQuery = ReactorQL.builder()
                .sql(INNER_SQL)
                .feature(coldOn(new AtomicInteger(), (value, context) ->
                        Mono.error(new IllegalStateException("ON failure"))))
                .build();
        StepVerifier.create(errorQuery.start(sources(1, 1)))
                    .expectErrorMatches(error -> error.getMessage().contains("ON failure"))
                    .verify();

        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger neverSubscriptions = new AtomicInteger();
        Sinks.One<Boolean> predicateSubscribed = Sinks.one();
        ReactorQL neverQuery = ReactorQL.builder()
                .sql(INNER_SQL)
                .feature(coldOn(neverSubscriptions, (value, context) ->
                        Mono.<Object>never()
                                .doOnSubscribe(subscription -> predicateSubscribed.tryEmitValue(true))
                                .doOnCancel(() -> cancelled.set(true))))
                .build();
        StepVerifier.create(neverQuery.start(sources(1, 1))
                                      .takeUntilOther(predicateSubscribed.asMono()), 0)
                    .thenRequest(1)
                    .verifyComplete();
        assertEquals(1, neverSubscriptions.get());
        assertTrue(cancelled.get());
    }

    @Test
    void shouldPreserveMetadataFlatMapAndExplicitConcurrency() {
        AtomicInteger metadataFlatMaps = new AtomicInteger();
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(INNER_SQL) {
            @Override
            public <S, T> Flux<T> flatMap(Flux<S> source,
                                          Function<S, ? extends Publisher<? extends T>> mapper) {
                metadataFlatMaps.incrementAndGet();
                return source.flatMap(mapper, getConcurrency());
            }
        };
        metadata.addFeature(coldOn(new AtomicInteger(), (value, context) -> Mono.just(value)));
        List<Map<String, Object>> rows = new DefaultReactorQL(metadata).start(sources(1, 1)).collectList().block();
        assertEquals(Collections.singletonList(result(1, 1)), rows);
        assertTrue(metadataFlatMaps.get() > 0);

        ReactorQL configured = ReactorQL.builder()
                .sql(INNER_SQL)
                .setting("concurrency", "invalid")
                .feature(coldOn(new AtomicInteger(), (value, context) -> Mono.just(value)))
                .build();
        StepVerifier.create(configured.start(sources(1, 1)))
                    .expectError()
                    .verify();
    }

    private static ValueMapFeature coldOn(AtomicInteger subscriptions,
                                          BiFunction<Object, ContextView, Mono<Object>> response) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                net.sf.jsqlparser.expression.Function function =
                        (net.sf.jsqlparser.expression.Function) expression;
                Function<ReactorQLRecord, Publisher<?>> key = ValueMapFeature.createMapperNow(
                        function.getParameters().getExpressions().get(0), metadata);
                return record -> Mono.deferContextual(context -> {
                    subscriptions.incrementAndGet();
                    return Mono.from(key.apply(record)).flatMap(value -> response.apply(value, context));
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_on").getId();
            }
        };
    }

    private static Function<String, Publisher<?>> sources(int left, int... right) {
        List<Map<String, Object>> rightRows = new ArrayList<>(right.length);
        for (int id : right) {
            rightRows.add(Collections.<String, Object>singletonMap("id", id));
        }
        return name -> "t1".equals(name)
                ? Flux.just(Collections.<String, Object>singletonMap("id", left))
                : "t2".equals(name) ? Flux.fromIterable(rightRows) : Flux.empty();
    }

    private static Map<String, Object> result(Integer left, Integer right) {
        Map<String, Object> value = new HashMap<>();
        if (left != null) {
            value.put("left_id", left);
        }
        if (right != null) {
            value.put("right_id", right);
        }
        return value;
    }
}
