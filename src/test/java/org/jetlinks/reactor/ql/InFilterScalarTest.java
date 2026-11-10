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
import org.jetlinks.reactor.ql.supports.filter.InFilter;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InFilterScalarTest {

    @Test
    void shouldKeepLiteralInNotInNullAndCheckpointResults() {
        ReactorQL in = ReactorQL.builder()
                               .sql("select this value from test where this in (1,2,null,4)")
                               .build();
        StepVerifier.create(in.start(Flux.range(0, 6)))
                    .expectNext(Collections.singletonMap("value", 1))
                    .expectNext(Collections.singletonMap("value", 2))
                    .expectNext(Collections.singletonMap("value", 4))
                    .verifyComplete();

        ReactorQL notIn = ReactorQL.builder()
                                  .sql("select this value from test where this not in (1,2,null,4)")
                                  .build();
        StepVerifier.create(notIn.start(Flux.range(0, 6)))
                    .expectNext(Collections.singletonMap("value", 0))
                    .expectNext(Collections.singletonMap("value", 3))
                    .expectNext(Collections.singletonMap("value", 5))
                    .verifyComplete();

        ReactorQL checkpoint = ReactorQL.builder()
                                       .setting("checkpoint", true)
                                       .sql("select this value from test where this in (1,2,null,4)")
                                       .build();
        StepVerifier.create(checkpoint.start(Flux.range(0, 6)))
                    .expectNext(Collections.singletonMap("value", 1))
                    .expectNext(Collections.singletonMap("value", 2))
                    .expectNext(Collections.singletonMap("value", 4))
                    .verifyComplete();
    }

    @Test
    void shouldKeepIterableMapAndPublisherLeftValues() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger multiValueSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                                  .sql("select id from test where choice in ('online','unknown')")
                                  .build();
        Map<String, Object> iterable = row(1, Arrays.asList("offline", "online"));
        Map<String, Object> map = row(2, Collections.singletonMap("entry", "online"));
        Map<String, Object> publisher = row(3, Mono.deferContextual(context -> {
            subscriptions.incrementAndGet();
            return Mono.just(context.get("choice"));
        }));
        Map<String, Object> unmatched = row(4, Flux.just("offline", "idle"));
        Map<String, Object> multiValue = row(5, Flux.defer(() -> {
            multiValueSubscriptions.incrementAndGet();
            return Flux.just("offline", "online");
        }));

        StepVerifier.create(query.start(Flux.just(iterable, map, publisher, unmatched, multiValue))
                                 .contextWrite(Context.of("choice", "online")))
                    .expectNext(Collections.singletonMap("id", 1))
                    .expectNext(Collections.singletonMap("id", 2))
                    .expectNext(Collections.singletonMap("id", 3))
                    .expectNext(Collections.singletonMap("id", 5))
                    .verifyComplete();
        assertEquals(1, subscriptions.get());
        assertEquals(1, multiValueSubscriptions.get());
    }

    @Test
    void shouldKeepPublisherLeftValueInsideNestedBooleanConditions() {
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                                  .sql("select id from test where (choice in ('online') and id > 0) or id < 0")
                                  .build();
        Map<String, Object> publisher = row(1, Mono.deferContextual(context -> {
            subscriptions.incrementAndGet();
            return Mono.just(context.get("choice"));
        }));
        StepVerifier.create(query.start(Flux.just(publisher, row(2, Arrays.asList("offline"))))
                                 .contextWrite(Context.of("choice", "online")))
                    .expectNext(Collections.singletonMap("id", 1))
                    .verifyComplete();
        assertEquals(1, subscriptions.get());
    }

    @Test
    void overridableInFilterMustKeepEmptyPublisherFallback() {
        InFilter emptySubclass = new InFilter() {
            @Override
            protected Mono<Boolean> doPredicate(boolean not, Flux<Object> left, Flux<Object> values) {
                return Mono.empty();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                  .feature(emptySubclass)
                                  .sql("select id from test where choice in ('online') or id = 1")
                                  .build();
        StepVerifier.create(query.start(Flux.just(row(1, Arrays.asList("online")))))
                    .expectNext(Collections.singletonMap("id", 1))
                    .verifyComplete();
    }

    @Test
    void shouldKeepColdPublisherCancellationAndError() {
        Sinks.One<Boolean> subscribed = Sinks.one();
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL.builder()
                                  .sql("select id from test where choice in ('online')")
                                  .build();
        Publisher<String> never = Mono.<String>never()
                                      .doOnSubscribe(ignore -> subscribed.tryEmitValue(true))
                                      .doOnCancel(() -> cancelled.set(true));
        StepVerifier.create(query.start(Flux.just(row(1, never)))
                                 .takeUntilOther(subscribed.asMono()), 0)
                    .thenRequest(1)
                    .verifyComplete();
        assertTrue(cancelled.get());

        StepVerifier.create(query.start(Flux.just(row(1, Mono.error(new IllegalStateException("choice failure"))))))
                    .expectErrorMatches(error -> error.getMessage().contains("choice failure"))
                    .verify();
    }

    @Test
    void shouldKeepAsyncRightMapperAndSubqueryFallback() {
        AtomicInteger subscriptions = new AtomicInteger();
        ValueMapFeature coldRight = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.deferContextual(context -> {
                    subscriptions.incrementAndGet();
                    return Mono.just(context.get("choice"));
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_right").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                  .feature(coldRight)
                                  .sql("select id from test where choice in (cold_right())")
                                  .build();
        StepVerifier.create(query.start(Flux.just(row(1, "online"), row(2, "offline")))
                                 .contextWrite(Context.of("choice", "online")))
                    .expectNext(Collections.singletonMap("id", 1))
                    .verifyComplete();
        assertEquals(2, subscriptions.get());

        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL subquery = ReactorQL.builder()
                                      .sql("select id from outer_table where id in (select value from lookup)")
                                      .build();
        StepVerifier.create(subquery.start(name -> "lookup".equals(name)
                ? Flux.just(Collections.singletonMap("value", 2))
                      .doOnSubscribe(ignore -> lookupSubscriptions.incrementAndGet())
                : "outer_table".equals(name)
                        ? Flux.just(row(1, null), row(2, null))
                        : Flux.empty()))
                    .expectNext(Collections.singletonMap("id", 2))
                    .verifyComplete();
        assertTrue(lookupSubscriptions.get() > 0);
    }

    @Test
    void shouldKeepDynamicRightNotInNullAndErrorSemantics() {
        AtomicInteger subscriptions = new AtomicInteger();
        ValueMapFeature coldRight = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.deferContextual(context -> {
                    subscriptions.incrementAndGet();
                    return context.getOrDefault("fail", false)
                            ? Mono.error(new IllegalStateException("right failure"))
                            : Mono.just(context.get("choice"));
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_right").getId();
            }
        };
        ReactorQL notIn = ReactorQL.builder()
                                  .feature(coldRight)
                                  .sql("select id from test where choice not in (cold_right())")
                                  .build();
        StepVerifier.create(notIn.start(Flux.just(row(1, "online"), row(2, "offline"), row(3, null)))
                                 .contextWrite(Context.of("choice", "online")))
                    .expectNext(Collections.singletonMap("id", 2))
                    .expectNext(Collections.singletonMap("id", 3))
                    .verifyComplete();
        assertEquals(3, subscriptions.get());

        StepVerifier.create(notIn.start(Flux.just(row(4, null)))
                                 .contextWrite(Context.of("fail", true)))
                    .expectErrorMatches(error -> error.getMessage().contains("right failure"))
                    .verify();
    }

    @Test
    void shouldCancelColdDynamicRightForScalarLeft() {
        Sinks.One<Boolean> subscribed = Sinks.one();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature coldRight = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.never()
                                     .doOnSubscribe(ignore -> subscribed.tryEmitValue(true))
                                     .doOnCancel(() -> cancelled.set(true));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_right").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                  .feature(coldRight)
                                  .sql("select id from test where choice in (cold_right())")
                                  .build();
        StepVerifier.create(query.start(Flux.just(row(1, "online")))
                                 .takeUntilOther(subscribed.asMono()), 0)
                    .thenRequest(1)
                    .verifyComplete();
        assertTrue(cancelled.get());
    }

    @Test
    void shouldMatchNativeInResultPolarityAndLifecycle() {
        List<List<Object>> candidates = Arrays.asList(
                Collections.emptyList(),
                Collections.singletonList("offline"),
                Arrays.asList("offline", "online"),
                Arrays.asList("unknown", "online"));
        for (boolean not : new boolean[]{false, true}) {
            for (List<Object> right : candidates) {
                assertEquals(inLifecycle(not, right, true), inLifecycle(not, right, false),
                             "Native demand/Context/cancellation differs for not=" + not + ", right=" + right);
            }
        }
    }

    @Test
    void shouldKeepNativeInSourceErrorIdentity() {
        RuntimeException failure = new RuntimeException("native IN source failure");
        for (boolean not : new boolean[]{false, true}) {
            for (boolean failLeft : new boolean[]{false, true}) {
                for (boolean nativeChain : new boolean[]{false, true}) {
                    Flux<Object> left = failLeft ? Flux.error(failure) : Flux.just("online");
                    Flux<Object> right = failLeft ? Flux.just("online") : Flux.error(failure);
                    StepVerifier.create(nativeChain
                                                ? nativeIn(not, left, right)
                                                : new ExposedInFilter().predicate(not, left, right))
                                .expectErrorMatches(error -> error == failure)
                                .verify();
                }
            }
        }
    }

    private static List<String> inLifecycle(boolean not, List<Object> candidates, boolean nativeChain) {
        List<String> trace = new ArrayList<>();
        Flux<Object> left = tracedValues("left", Arrays.asList("online", "unknown"), trace);
        Flux<Object> right = tracedValues("right", candidates, trace);
        Mono<Boolean> result = nativeChain ? nativeIn(not, left, right)
                : new ExposedInFilter().predicate(not, left, right);
        boolean matched = candidates.contains("online") || candidates.contains("unknown");
        StepVerifier.create(result.contextWrite(Context.of("token", "native-in")), 0)
                    .then(() -> trace.add("request:1"))
                    .thenRequest(1)
                    .expectNext(not != matched)
                    .verifyComplete();
        return trace;
    }

    private static Flux<Object> tracedValues(String name, List<Object> values, List<String> trace) {
        return Flux.deferContextual(context -> {
            trace.add(name + ":context:" + context.get("token"));
            return Flux.fromIterable(values)
                       .doOnSubscribe(ignore -> trace.add(name + ":subscribe"))
                       .doOnRequest(request -> trace.add(name + ":request:" + request))
                       .doOnCancel(() -> trace.add(name + ":cancel"));
        });
    }

    // Keep the pre-optimization native composition independent of the production result branch.
    private static Mono<Boolean> nativeIn(boolean not, Flux<Object> left, Flux<Object> values) {
        Flux<Object> leftCache = left.replay().refCount(1);
        return values.flatMap(value -> leftCache.map(candidate -> CompareUtils.equals(value, candidate)))
                     .any(Boolean.TRUE::equals)
                     .map(matched -> not != matched);
    }

    private static class ExposedInFilter extends InFilter {
        private Mono<Boolean> predicate(boolean not, Flux<Object> left, Flux<Object> values) {
            return doPredicate(not, left, values);
        }
    }

    private static Map<String, Object> row(int id, Object choice) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        if (choice != null) {
            row.put("choice", choice);
        }
        return row;
    }
}
