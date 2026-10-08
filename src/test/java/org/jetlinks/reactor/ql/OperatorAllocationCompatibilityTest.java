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
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.group.GroupByBinaryFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Timeout(10)
class OperatorAllocationCompatibilityTest {

    private static final String CONTEXT_KEY = "marker";
    private static final String CONTEXT_VALUE = "visible";

    @Test
    void multipleAggregatesKeepAliasesMultiValuesAndSubscriptionIsolation() {
        AtomicInteger factories = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .feature(aggregate("probe", source -> {
                    factories.incrementAndGet();
                    return source.map(ReactorQLRecord::getRecord);
                }))
                .sql("select probe(this) values_a,probe(this) values_b,count(1) total from test")
                .build();
        Assertions.assertEquals(0, factories.get());
        Flux<Integer> source = Flux.deferContextual(context -> {
            Assertions.assertEquals(CONTEXT_VALUE, context.get(CONTEXT_KEY));
            subscriptions.incrementAndGet();
            return Flux.just(1, 2);
        });
        for (int iteration = 0; iteration < 2; iteration++) {
            StepVerifier.create(query.start(source).contextWrite(context -> context.put(CONTEXT_KEY, CONTEXT_VALUE)), 0)
                    .thenRequest(1)
                    .assertNext(result -> {
                        Assertions.assertEquals(Arrays.asList(1, 2), result.get("values_a"));
                        Assertions.assertEquals(Arrays.asList(1, 2), result.get("values_b"));
                        Assertions.assertEquals(2L, result.get("total"));
                        Assertions.assertEquals(3, result.size());
                    })
                    .verifyComplete();
        }
        Assertions.assertEquals(4, factories.get());
        Assertions.assertEquals(2, subscriptions.get());
    }

    @Test
    void multipleAggregateFactoryFailureKeepsEntryErrorBoundary() {
        RuntimeException failure = new RuntimeException("aggregate factory");
        AtomicInteger recovered = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .feature(aggregate("failure", source -> { throw failure; }))
                .sql("select failure(this) broken,failure(this) other from test")
                .build();
        StepVerifier.create(query.start(Flux.just(1)))
                .expectErrorMatches(error -> error == failure)
                .verify();
        StepVerifier.create(query.start(Flux.just(1)).onErrorContinue((error, value) -> {
            Assertions.assertSame(failure, error);
            Assertions.assertTrue(value instanceof Map.Entry);
            Assertions.assertTrue(Arrays.asList("broken", "other").contains(((Map.Entry<?, ?>) value).getKey()));
            recovered.incrementAndGet();
        }))
                .expectNext(Collections.emptyMap())
                .verifyComplete();
        Assertions.assertEquals(2, recovered.get());
    }

    @Test
    void multipleAggregatesCancelSharedSourceOnlyOnce() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .sql("select count(1) total,max(this) largest from test")
                .build();
        StepVerifier.create(query.start(Flux.<Integer>never()
                        .doOnSubscribe(ignore -> subscriptions.incrementAndGet())
                        .doOnCancel(cancellations::incrementAndGet)), 0)
                .thenRequest(1)
                .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                .thenCancel()
                .verify();
        Assertions.assertEquals(1, subscriptions.get());
        Assertions.assertEquals(1, cancellations.get());
    }

    @Test
    void binaryGroupingKeepsContextRecordIdentityAndColdSubscriptions() {
        AtomicInteger leftSubscriptions = new AtomicInteger();
        AtomicInteger rightSubscriptions = new AtomicInteger();
        Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> mapper = binaryGroups(
                record -> Mono.deferContextual(context -> {
                    Assertions.assertEquals(CONTEXT_VALUE, context.get(CONTEXT_KEY));
                    leftSubscriptions.incrementAndGet();
                    return Mono.just(7);
                }),
                record -> Mono.defer(() -> {
                    rightSubscriptions.incrementAndGet();
                    return Mono.just(3);
                }));
        for (int iteration = 0; iteration < 2; iteration++) {
            ReactorQLRecord record = record();
            StepVerifier.create(mapper.apply(Flux.just(record)).concatMap(Function.identity())
                            .contextWrite(context -> context.put(CONTEXT_KEY, CONTEXT_VALUE)), 0)
                    .thenRequest(1)
                    .assertNext(value -> {
                        Assertions.assertSame(record, value);
                        Assertions.assertEquals(Collections.singletonList(10), GroupFeature.getGroupKey(value));
                    })
                    .verifyComplete();
        }
        Assertions.assertEquals(2, leftSubscriptions.get());
        Assertions.assertEquals(2, rightSubscriptions.get());
    }

    @Test
    void binaryGroupingKeepsEmptyAndOriginalErrorSignals() {
        AtomicInteger rightSubscriptions = new AtomicInteger();
        StepVerifier.create(binaryGroups(record -> Mono.empty(), record -> Mono.just(3)
                        .doOnSubscribe(ignore -> rightSubscriptions.incrementAndGet()))
                        .apply(Flux.just(record())).concatMap(Function.identity()))
                .verifyComplete();
        Assertions.assertEquals(1, rightSubscriptions.get());
        RuntimeException failure = new RuntimeException("left");
        StepVerifier.create(binaryGroups(record -> Mono.error(failure), record -> Mono.just(3))
                        .apply(Flux.just(record())).concatMap(Function.identity()))
                .expectErrorMatches(error -> error == failure)
                .verify();
        StepVerifier.create(binaryGroups(record -> Mono.just(7), record -> Mono.error(failure))
                        .apply(Flux.just(record())).concatMap(Function.identity()))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }

    @Test
    void binaryGroupingCancelsBothPendingOperands() {
        TestPublisher<Integer> left = TestPublisher.create();
        TestPublisher<Integer> right = TestPublisher.create();
        StepVerifier.create(binaryGroups(record -> left.mono(), record -> right.mono())
                        .apply(Flux.just(record())).concatMap(Function.identity()), 0)
                .then(() -> {
                    left.assertSubscribers(1);
                    right.assertSubscribers(1);
                })
                .thenCancel()
                .verify();
        left.assertCancelled();
        right.assertCancelled();
    }

    @Test
    void checkpointBinarySqlKeepsEmptyKeysAndAggregateValues() {
        ReactorQL query = ReactorQL.builder().setting("checkpoint", true)
                .sql("select count(1) total from test group by score / 10")
                .build();
        Map<String, Object> first = Collections.singletonMap("score", 12);
        Map<String, Object> second = Collections.singletonMap("score", 19);
        StepVerifier.create(query.start(Flux.just(first, Collections.emptyMap(), second)), 0)
                .thenRequest(1)
                .expectNext(Collections.singletonMap("total", 2L))
                .verifyComplete();
    }

    private static ReactorQLRecord record() {
        return ReactorQLRecord.newRecord("test", new HashMap<>(), ReactorQLContext.ofDatasource(ignore -> Flux.empty()));
    }

    private static Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> binaryGroups(
            Function<ReactorQLRecord, Publisher<?>> left, Function<ReactorQLRecord, Publisher<?>> right) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select * from test group by left_key() + right_key()");
        metadata.addFeature(value("left_key", left), value("right_key", right));
        return new GroupByBinaryFeature("+", (a, b) -> ((Number) a).intValue() + ((Number) b).intValue())
                .createGroupMapper(metadata.getSql().getGroupBy().getGroupByExpressionList().getExpressions().get(0), metadata);
    }

    private static ValueMapFeature value(String name, Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }

    private static ValueAggMapFeature aggregate(String name, Function<Flux<ReactorQLRecord>, Flux<Object>> mapper) {
        return new ValueAggMapFeature() {
            @Override
            public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return FeatureId.ValueAggMap.of(name).getId();
            }
        };
    }
}
