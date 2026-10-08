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
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class LegacyMultiValueAggregateTest {

    @Test
    void shouldKeepMultiValueTypeAndSingleValueShape() {
        ReactorQL query = query(rows -> rows.map(ReactorQLRecord::getRecord));

        StepVerifier.create(query.start(Flux.range(0, 4)))
                    .assertNext(result -> {
                        Assertions.assertEquals(4L, result.get("total"));
                        Assertions.assertInstanceOf(CopyOnWriteArrayList.class, result.get("emitted"));
                        Assertions.assertEquals(Arrays.asList(0, 1, 2, 3), result.get("emitted"));
                    })
                    .verifyComplete();

        StepVerifier.create(query.start(Flux.just(5)))
                    .assertNext(result -> {
                        Assertions.assertEquals(1L, result.get("total"));
                        Assertions.assertEquals(5, result.get("emitted"));
                    })
                    .verifyComplete();
    }

    @Test
    void shouldPreserveFirstListValueMutation() {
        List<Object> first = new ArrayList<>(Arrays.asList("a", "b"));
        ReactorQL query = query(rows -> rows.take(1)
                                            .flatMap(ignore -> Flux.just((Object) first, "c")));

        StepVerifier.create(query.start(Flux.just(1)))
                    .assertNext(result -> {
                        Assertions.assertSame(first, result.get("emitted"));
                        Assertions.assertEquals(Arrays.asList("a", "b", "c"), first);
                    })
                    .verifyComplete();
    }

    @Test
    void shouldSerializeAsyncValuesAndPropagateErrorAndCancellation() {
        ReactorQL query = query(rows -> rows.publishOn(Schedulers.parallel())
                                           .map(ReactorQLRecord::getRecord));

        StepVerifier.create(query.start(Flux.range(0, 128)))
                    .assertNext(result -> {
                        Assertions.assertEquals(128L, result.get("total"));
                        Assertions.assertInstanceOf(CopyOnWriteArrayList.class, result.get("emitted"));
                        List<?> emitted = (List<?>) result.get("emitted");
                        Assertions.assertEquals(128, emitted.size());
                        Assertions.assertEquals(0, emitted.get(0));
                        Assertions.assertEquals(127, emitted.get(127));
                    })
                    .verifyComplete();

        RuntimeException failure = new RuntimeException("source failed");
        StepVerifier.create(query.start(Flux.concat(Flux.just(1), Flux.error(failure))))
                    .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                    .verify();

        ReactorQL synchronous = query(rows -> rows.map(ReactorQLRecord::getRecord));
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        StepVerifier.create(synchronous.start(Flux.range(0, 10)
                                                  .concatWith(Flux.never())
                                                  .doOnSubscribe(ignore -> subscribed.set(true))
                                                  .doOnCancel(() -> cancelled.set(true))), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldIsolateConcurrentAggregateResultsAndContextAcrossSubscriptions() {
        ReactorQL query = ReactorQL.builder()
                .feature(feature("emit_each", rows -> rows
                        .transformDeferredContextual((source, context) -> source
                                .publishOn(Schedulers.parallel())
                                .map(row -> (Object) ((Integer) row.getRecord()
                                        + context.<Integer>get("offset"))))))
                .feature(feature("emit_other", rows -> rows
                        .transformDeferredContextual((source, context) -> source
                                .publishOn(Schedulers.parallel())
                                .map(row -> (Object) ((Integer) row.getRecord()
                                        - context.<Integer>get("offset"))))))
                .sql("select emit_each(this) emitted,emit_other(this) other,count(1) total from test")
                .build();
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Integer> source = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.range(0, 1024);
        });

        for (int offset : new int[]{10, 20}) {
            StepVerifier.create(query.start(source).contextWrite(context -> context.put("offset", offset)), 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .assertNext(result -> {
                        Assertions.assertEquals(3, result.size());
                        Assertions.assertEquals(1024L, result.get("total"));
                        Assertions.assertInstanceOf(CopyOnWriteArrayList.class, result.get("emitted"));
                        Assertions.assertInstanceOf(CopyOnWriteArrayList.class, result.get("other"));
                        List<?> emitted = (List<?>) result.get("emitted");
                        List<?> other = (List<?>) result.get("other");
                        Assertions.assertNotSame(emitted, other);
                        Assertions.assertEquals(1024, emitted.size());
                        Assertions.assertEquals(1024, other.size());
                        for (int index = 0; index < 1024; index++) {
                            Assertions.assertEquals(index + offset, emitted.get(index));
                            Assertions.assertEquals(index - offset, other.get(index));
                        }
                    })
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
        }
        Assertions.assertEquals(2, subscriptions.get());
    }

    @Test
    void shouldKeepEmptySourceAndResultDemandSemantics() {
        ReactorQL query = query(rows -> rows.map(ReactorQLRecord::getRecord));
        StepVerifier.create(query.start(Flux.empty()), 0)
                .expectSubscription()
                .thenRequest(1)
                .assertNext(result -> {
                    Assertions.assertEquals(1, result.size());
                    Assertions.assertEquals(0L, result.get("total"));
                    Assertions.assertFalse(result.containsKey("emitted"));
                })
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        StepVerifier.create(query.start(Flux.just(7, 8)), 0)
                .expectSubscription()
                .thenRequest(1)
                .assertNext(result -> {
                    Assertions.assertEquals(2L, result.get("total"));
                    Assertions.assertInstanceOf(CopyOnWriteArrayList.class, result.get("emitted"));
                    Assertions.assertEquals(Arrays.asList(7, 8), result.get("emitted"));
                })
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void shouldPreserveExpandedMapOverwriteOrder() {
        Map<String, Object> expansion = new LinkedHashMap<>();
        StringBuilder sql = new StringBuilder("select emit_map(this) \"$this\"");
        for (int index = 0; index < 11; index++) {
            String alias = "a" + index;
            expansion.put(alias, 0L);
            sql.append(",count(1) ").append(alias);
        }
        sql.append(" from test");
        // 对照原 collector 的实际 compute/遍历边界；12 个别名触发不同 Map 的扩容差异。
        Map<String, Object> original = new ConcurrentHashMap<>();
        original.compute("$this", (name, previous) -> expansion);
        expansion.forEach((name, ignored) -> original.compute(name, (key, previous) -> 1L));
        Map<String, Object> expected = new HashMap<>();
        original.forEach((name, value) -> {
            if ("$this".equals(name)) {
                expected.putAll(expansion);
            } else {
                expected.put(name, value);
            }
        });
        ReactorQL query = ReactorQL.builder()
                .feature(feature("emit_map", rows -> rows.take(1).map(row -> (Object) expansion)))
                .sql(sql.toString())
                .build();

        StepVerifier.create(query.start(Flux.just(1)))
                .expectNext(expected)
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

    private static ReactorQL query(Function<Flux<ReactorQLRecord>, Flux<Object>> aggregate) {
        return ReactorQL.builder()
                        .feature(feature("emit_each", aggregate))
                        .sql("select emit_each(this) emitted,count(1) total from test")
                        .build();
    }

    private static ValueAggMapFeature feature(String name,
                                             Function<Flux<ReactorQLRecord>, Flux<Object>> aggregate) {
        return new ValueAggMapFeature() {
            @Override
            public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression,
                                                                                ReactorQLMetadata metadata) {
                return aggregate;
            }

            @Override
            public String getId() {
                return FeatureId.ValueAggMap.of(name).getId();
            }
        };
    }
}
