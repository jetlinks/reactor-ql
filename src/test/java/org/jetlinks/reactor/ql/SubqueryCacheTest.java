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

import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.internal.SubscriptionContext;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class SubqueryCacheTest {

    private static final int ABOVE_PREVIOUS_DEFAULT = 65_537;

    @Test
    void shouldPreserveUnboundedCachedSubqueryDefault() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id from outer_table o where o.id in (select value from lookup)")
                .build();

        query.start(sources(lookupSubscriptions, 1, ABOVE_PREVIOUS_DEFAULT))
             .as(StepVerifier::create)
             .expectNext(Collections.singletonMap("o.id", 0))
             .verifyComplete();
        Assertions.assertEquals(1, lookupSubscriptions.get());
        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("maxRows=unbounded"));
    }

    @Test
    void shouldExecuteUncorrelatedSubqueryOncePerSubscription() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id,(select value from lookup) cached from outer_table o")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query)
                                      .describeExecutionPlan()
                                      .contains("subquery-cache"));

        Function<String, Publisher<?>> sources = sources(lookupSubscriptions, 10, 1);
        query.start(sources)
             .as(StepVerifier::create)
             .expectNextCount(10)
             .verifyComplete();
        Assertions.assertEquals(1, lookupSubscriptions.get());

        query.start(sources)
             .as(StepVerifier::create)
             .expectNextCount(10)
             .verifyComplete();
        Assertions.assertEquals(2, lookupSubscriptions.get());
    }

    @Test
    void shouldReadCompletedMultiValueCacheWithoutResubscribing() {
        SubscriptionContext context = new SubscriptionContext();
        Object key = new Object();
        Object first = new Object();
        Object second = new Object();
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Object> values = context.cacheMany(key, () -> Flux.just(first, second)
                .doOnSubscribe(ignore -> subscriptions.incrementAndGet()), 2);

        StepVerifier.create(values).expectNext(first, second).verifyComplete();
        StepVerifier.create(values, 0)
                    .thenRequest(1)
                    .expectNext(first)
                    .thenRequest(1)
                    .expectNext(second)
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void shouldReadCompletedSingleValueCacheWithDemandAndContext() {
        SubscriptionContext context = new SubscriptionContext();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean contextVisible = new AtomicBoolean();
        Object value = new Object();
        Flux<Object> values = context.cacheMany(new Object(), () -> Flux.just(value)
                .doOnSubscribe(ignore -> subscriptions.incrementAndGet()), 2);

        StepVerifier.create(values).expectNext(value).verifyComplete();
        StepVerifier.create(values.doOnEach(signal -> {
                        if (signal.isOnNext()) {
                            contextVisible.set("reader".equals(signal.getContextView().get("marker")));
                        }
                    })
                    .contextWrite(Context.of("marker", "reader")), 0)
                    .thenRequest(1)
                    .expectNext(value)
                    .verifyComplete();
        Assertions.assertTrue(contextVisible.get());
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void shouldReplayCompletedEmptyMultiValueCacheWithoutResubscribing() {
        SubscriptionContext context = new SubscriptionContext();
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Integer> values = context.cacheMany(new Object(), () -> Flux.<Integer>empty()
                .doOnSubscribe(ignore -> subscriptions.incrementAndGet()), 2);

        StepVerifier.create(values).verifyComplete();
        StepVerifier.create(values, 0)
                    .thenRequest(1)
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void shouldReplayCompletedEmptyMonoCacheWithoutResubscribing() {
        SubscriptionContext context = new SubscriptionContext();
        AtomicInteger subscriptions = new AtomicInteger();
        Mono<Integer> value = context.cacheMono(new Object(), () -> Mono.<Integer>empty()
                .doOnSubscribe(ignore -> subscriptions.incrementAndGet()));

        StepVerifier.create(value).verifyComplete();
        StepVerifier.create(value, 0)
                    .thenRequest(1)
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void shouldReuseCachedPublishersAcrossLookups() {
        SubscriptionContext context = new SubscriptionContext();
        Object manyKey = new Object();
        Object monoKey = new Object();
        AtomicInteger manySources = new AtomicInteger();
        AtomicInteger monoSources = new AtomicInteger();

        Flux<Integer> many = context.cacheMany(manyKey, () -> Flux.defer(() -> {
            manySources.incrementAndGet();
            return Flux.just(1, 2);
        }), 2);
        Assertions.assertSame(many, context.cacheMany(manyKey, () -> Flux.error(
                new AssertionError("cached multi-value source must not be replaced")), 2));
        StepVerifier.create(many).expectNext(1, 2).verifyComplete();
        StepVerifier.create(context.cacheMany(manyKey, Flux::empty, 2))
                    .expectNext(1, 2)
                    .verifyComplete();
        Assertions.assertEquals(1, manySources.get());

        Mono<Integer> single = context.cacheMono(monoKey, () -> Mono.defer(() -> {
            monoSources.incrementAndGet();
            return Mono.just(3);
        }));
        Assertions.assertSame(single, context.cacheMono(monoKey, () -> Mono.error(
                new AssertionError("cached single-value source must not be replaced"))));
        StepVerifier.create(single).expectNext(3).verifyComplete();
        StepVerifier.create(context.cacheMono(monoKey, Mono::empty))
                    .expectNext(3)
                    .verifyComplete();
        Assertions.assertEquals(1, monoSources.get());
    }

    @Test
    void shouldKeepPendingMultiValueCacheSharedAndRestartAfterCancellation() {
        SubscriptionContext context = new SubscriptionContext();
        Object key = new Object();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        Flux<Integer> values = context.cacheMany(key, () -> Flux.defer(() -> {
            int attempt = subscriptions.incrementAndGet();
            return attempt == 1
                    ? Flux.<Integer>never().doOnCancel(() -> cancelled.set(true))
                    : Flux.just(1, 2);
        }), 2);

        StepVerifier.create(values, 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
        StepVerifier.create(values).expectNext(1, 2).verifyComplete();
        StepVerifier.create(values).expectNext(1, 2).verifyComplete();
        Assertions.assertEquals(2, subscriptions.get());
    }

    @Test
    void shouldKeepPendingMonoCacheRestartableAfterCancellation() {
        SubscriptionContext context = new SubscriptionContext();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        Mono<Integer> value = context.cacheMono(new Object(), () -> Mono.defer(() -> {
            int attempt = subscriptions.incrementAndGet();
            return attempt == 1
                    ? Mono.<Integer>never().doOnCancel(() -> cancelled.set(true))
                    : Mono.just(1);
        }));

        StepVerifier.create(value, 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
        StepVerifier.create(value).expectNext(1).verifyComplete();
        StepVerifier.create(value).expectNext(1).verifyComplete();
        Assertions.assertEquals(2, subscriptions.get());
    }

    @Test
    void shouldSharePendingMultiValueCacheAcrossConcurrentReaders() {
        SubscriptionContext context = new SubscriptionContext();
        Object key = new Object();
        AtomicInteger subscriptions = new AtomicInteger();
        Sinks.One<Integer> pending = Sinks.one();
        Flux<Integer> values = context.cacheMany(key, () -> pending.asMono()
                .flux()
                .doOnSubscribe(ignore -> subscriptions.incrementAndGet()), 2);

        StepVerifier.create(Mono.zip(values.collectList(), values.collectList()))
                    .expectSubscription()
                    .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                    .then(() -> pending.tryEmitValue(7))
                    .assertNext(result -> {
                        Assertions.assertEquals(Collections.singletonList(7), result.getT1());
                        Assertions.assertEquals(Collections.singletonList(7), result.getT2());
                    })
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void shouldNotPublishMultiValueCacheAfterSourceError() {
        SubscriptionContext context = new SubscriptionContext();
        Object key = new Object();
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Integer> values = context.cacheMany(key, () -> Flux.concat(
                Flux.just(1), Flux.error(new IllegalStateException("lookup failed")))
                .doOnSubscribe(ignore -> subscriptions.incrementAndGet()), 2);

        StepVerifier.create(values).expectErrorMessage("lookup failed").verify();
        StepVerifier.create(values).expectErrorMessage("lookup failed").verify();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void shouldEvaluateMultipleCachedSubqueryColumns() {
        String[] queries = {
                "select id,(select value from lookup) first_cached,"
                        + "(select value from lookup) second_cached from outer_table",
                "select id,(select value from lookup) first_cached,"
                        + "(select value from lookup) second_cached,"
                        + "(select value from lookup) third_cached from outer_table"
        };
        for (int index = 0; index < queries.length; index++) {
            int columns = index + 2;
            AtomicInteger lookupSubscriptions = new AtomicInteger();
            ReactorQL query = ReactorQL.builder().sql(queries[index]).build();
            query.start(sources(lookupSubscriptions, 2, 1))
                 .as(StepVerifier::create)
                 .assertNext(result -> assertCachedColumns(result, 0, columns))
                 .assertNext(result -> assertCachedColumns(result, 1, columns))
                 .verifyComplete();
            Assertions.assertEquals(columns, lookupSubscriptions.get());
        }
    }

    private static void assertCachedColumns(Map<String, Object> result, int id, int columns) {
        Assertions.assertEquals(id, result.get("id"));
        Map<String, Object> cached = Collections.singletonMap("value", 0);
        Assertions.assertEquals(cached, result.get("first_cached"));
        Assertions.assertEquals(cached, result.get("second_cached"));
        if (columns == 3) {
            Assertions.assertEquals(cached, result.get("third_cached"));
        }
    }

    @Test
    void shouldShareNestedUncorrelatedSubquery() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id,(select n.value from (select value from lookup) n) cached "
                             + "from outer_table o")
                .build();

        query.start(sources(lookupSubscriptions, 20, 1))
             .collectList()
             .as(StepVerifier::create)
             .assertNext(rows -> assertNestedRows(rows, "n.value"))
             .verifyComplete();
        Assertions.assertEquals(1, lookupSubscriptions.get());
    }

    @Test
    void shouldSharePureNestedAggregateOnlyWithinEachSubscription() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .sql("select o.id,(select sum(n.value) total "
                             + "from (select value from lookup) n) lookup_total "
                             + "from outer_table o")
                .build();

        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                                      .contains("subquery-cache"));
        Function<String, Publisher<?>> source = sources(lookupSubscriptions, 20, 4);
        for (int subscription = 1; subscription <= 2; subscription++) {
            query.start(source)
                 .collectList()
                 .as(StepVerifier::create)
                 .assertNext(rows -> {
                     Assertions.assertEquals(20, rows.size());
                     for (int index = 0; index < rows.size(); index++) {
                         Map<String, Object> row = rows.get(index);
                         Assertions.assertEquals(index, row.get("o.id"));
                         Map<?, ?> aggregate = Assertions.assertInstanceOf(
                                 Map.class, row.get("lookup_total"));
                         Assertions.assertEquals(Collections.singleton("total"), aggregate.keySet());
                         Assertions.assertEquals(6.0, aggregate.get("total"));
                     }
                 })
                 .verifyComplete();
            Assertions.assertEquals(subscription, lookupSubscriptions.get());
        }
    }

    @Test
    void shouldKeepVolatileOrOverriddenAggregateOnPerRowPath() {
        AtomicInteger volatileSubscriptions = new AtomicInteger();
        AtomicInteger probeCalls = new AtomicInteger();
        ReactorQL volatileQuery = ReactorQL.builder()
                .feature(FunctionMapFeature.scalar("probe", 1, 1, values -> {
                    probeCalls.incrementAndGet();
                    return values.get(0);
                }))
                .sql("select o.id,(select count(probe(value)) total from lookup) nested "
                             + "from outer_table o")
                .build();
        Assertions.assertFalse(((DefaultReactorQL) volatileQuery).describeExecutionPlan()
                                       .contains("subquery-cache"));
        volatileQuery.start(sources(volatileSubscriptions, 5, 1))
                     .as(StepVerifier::create)
                     .expectNextCount(5)
                     .verifyComplete();
        Assertions.assertEquals(5, volatileSubscriptions.get());
        Assertions.assertEquals(5, probeCalls.get());

        AtomicInteger overriddenSubscriptions = new AtomicInteger();
        ReactorQL overridden = ReactorQL.builder()
                .feature(new CountAggFeature())
                .sql("select o.id,(select count(1) total from lookup) nested "
                             + "from outer_table o")
                .build();
        Assertions.assertFalse(((DefaultReactorQL) overridden).describeExecutionPlan()
                                       .contains("subquery-cache"));
        overridden.start(sources(overriddenSubscriptions, 5, 1))
                  .as(StepVerifier::create)
                  .expectNextCount(5)
                  .verifyComplete();
        Assertions.assertEquals(5, overriddenSubscriptions.get());
    }

    @Test
    void shouldShareThreeNestedUncorrelatedSubqueries() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id,(select n2.value from ("
                             + "select n1.value AS value from (select value from lookup) n1"
                             + ") n2) cached from outer_table o")
                .build();

        query.start(sources(lookupSubscriptions, 20, 1))
             .collectList()
             .as(StepVerifier::create)
             .assertNext(rows -> assertNestedRows(rows, "n2.value"))
             .verifyComplete();
        Assertions.assertEquals(1, lookupSubscriptions.get());
    }

    @Test
    void shouldNotCacheCorrelatedOrExplicitlyDisabledSubquery() {
        AtomicInteger correlatedSubscriptions = new AtomicInteger();
        ReactorQL correlated = ReactorQL
                .builder()
                .sql("select o.id,(select o.id value from lookup) nested from outer_table o")
                .build();
        ReactorQL disabled = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, false)
                .sql("select o.id,(select value from lookup) nested from outer_table o")
                .build();

        Assertions.assertFalse(((DefaultReactorQL) correlated)
                                       .describeExecutionPlan()
                                       .contains("subquery-cache"));
        correlated.start(sources(correlatedSubscriptions, 5, 1))
                  .as(StepVerifier::create)
                  .expectNextCount(5)
                  .verifyComplete();
        Assertions.assertEquals(5, correlatedSubscriptions.get());

        AtomicInteger disabledSubscriptions = new AtomicInteger();
        disabled.start(sources(disabledSubscriptions, 5, 1))
                .as(StepVerifier::create)
                .expectNextCount(5)
                .verifyComplete();
        Assertions.assertEquals(5, disabledSubscriptions.get());
    }

    @Test
    void shouldFailBeforeCachingUnboundedSubqueryResult() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_SUBQUERY_MAX_ROWS, 2)
                .sql("select o.id from outer_table o "
                             + "where o.id in (select value from lookup)")
                .build();

        query.start(sources(lookupSubscriptions, 3, 3))
             .as(StepVerifier::create)
             .expectErrorMatches(error -> error instanceof ReactorQLException
                     && error.getMessage().contains(DefaultReactorQL.SETTING_SUBQUERY_MAX_ROWS))
             .verify();
        Assertions.assertEquals(1, lookupSubscriptions.get());
    }

    @Test
    void shouldCacheUncorrelatedExistsBooleanAndCancelAfterFirstRow() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        AtomicBoolean lookupCancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id from outer_table o "
                             + "where exists(select value from lookup)")
                .build();

        Function<String, Publisher<?>> sources = name -> {
            if ("lookup".equals(name)) {
                return Flux
                        .concat(
                                Flux.just(Collections.<String, Object>singletonMap("value", 1)),
                                Flux.never()
                        )
                        .doOnSubscribe(ignore -> lookupSubscriptions.incrementAndGet())
                        .doOnCancel(() -> lookupCancelled.set(true));
            }
            return "outer_table".equals(name)
                    ? Flux.range(0, 10).map(SubqueryCacheTest::outerRow)
                    : Flux.empty();
        };

        query.start(sources)
             .as(StepVerifier::create)
             .expectNextCount(10)
             .verifyComplete();
        Assertions.assertEquals(1, lookupSubscriptions.get());
        Assertions.assertTrue(lookupCancelled.get());

        query.start(sources)
             .as(StepVerifier::create)
             .expectNextCount(10)
             .verifyComplete();
        Assertions.assertEquals(2, lookupSubscriptions.get());
    }

    @Test
    void shouldNotCacheCorrelatedExistsAcrossOuterRows() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id from outer_table o where exists("
                             + "select value from lookup where value = o.id)")
                .build();

        query.start(sources(lookupSubscriptions, 3, 3))
             .as(StepVerifier::create)
             .expectNextCount(3)
             .verifyComplete();
        Assertions.assertEquals(3, lookupSubscriptions.get());
    }

    @Test
    void shouldCancelPendingExistsWithRootSubscription() {
        AtomicInteger lookupSubscriptions = new AtomicInteger();
        AtomicBoolean lookupCancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL
                .builder()
                .sql("select o.id from outer_table o "
                             + "where exists(select value from lookup)")
                .build();

        query.start(name -> {
                 if ("lookup".equals(name)) {
                     return Flux
                             .never()
                             .doOnSubscribe(ignore -> lookupSubscriptions.incrementAndGet())
                             .doOnCancel(() -> lookupCancelled.set(true));
                 }
                 return "outer_table".equals(name) ? Flux.just(outerRow(1)) : Flux.empty();
             })
             .as(flux -> StepVerifier.create(flux, 0))
             .thenRequest(1)
             .then(() -> Assertions.assertEquals(1, lookupSubscriptions.get()))
             .thenCancel()
             .verify();
        Assertions.assertTrue(lookupCancelled.get());
    }

    private static Function<String, Publisher<?>> sources(AtomicInteger lookupSubscriptions,
                                                          int outerRows,
                                                          int lookupRows) {
        return name -> {
            if ("lookup".equals(name)) {
                return Flux
                        .range(0, lookupRows)
                        .map(value -> Collections.<String, Object>singletonMap("value", value))
                        .doOnSubscribe(ignore -> lookupSubscriptions.incrementAndGet());
            }
            if ("outer_table".equals(name)) {
                return Flux.range(0, outerRows).map(SubqueryCacheTest::outerRow);
            }
            return Flux.empty();
        };
    }

    private static void assertNestedRows(List<Map<String, Object>> rows, String cachedKey) {
        Assertions.assertEquals(20, rows.size());
        for (int index = 0; index < rows.size(); index++) {
            Map<String, Object> row = rows.get(index);
            Assertions.assertEquals(Arrays.asList("o.id", "cached"), new java.util.ArrayList<>(row.keySet()));
            Assertions.assertEquals(index, Assertions.assertInstanceOf(Integer.class, row.get("o.id")));
            Map<?, ?> cached = Assertions.assertInstanceOf(Map.class, row.get("cached"));
            Assertions.assertEquals(Collections.singleton(cachedKey), cached.keySet());
            Assertions.assertEquals(0, Assertions.assertInstanceOf(Integer.class, cached.get(cachedKey)));
        }
    }

    private static Map<String, Object> outerRow(int id) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        return row;
    }
}
