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

import org.jetlinks.reactor.ql.internal.GroupStateBudget;
import org.jetlinks.reactor.ql.internal.SubscriptionContext;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

class GroupBudgetLifecycleTest {

    @Test
    void shouldPreserveCompletedGroupValuesAndIdentity() {
        List<Flux<String>> retained = new ArrayList<>();
        List<List<String>> results = groups(2).apply(Flux.just("a", "b", "a"))
                .flatMap(group -> {
                    retained.add(group);
                    return group.collectList();
                }).collectList().block();
        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains(Arrays.asList("a", "a")));
        Assertions.assertTrue(results.contains(Collections.unmodifiableList(Arrays.asList("b"))));
        Assertions.assertEquals(2, retained.size());
        for (Flux<String> group : retained) {
            Assertions.assertTrue(group instanceof GroupedFlux);
            Assertions.assertTrue(Arrays.asList("a", "b").contains(((GroupedFlux<?, ?>) group).key()));
        }
    }

    @Test
    void shouldContinueSelectedGroupAfterOuterCancellation() {
        Flux<String> selected = groups(1).apply(Flux.just("a", "b", "a", "b", "a"))
                .next().block();
        Assertions.assertNotNull(selected);
        Assertions.assertTrue(selected instanceof GroupedFlux);
        Assertions.assertEquals("a", ((GroupedFlux<?, ?>) selected).key());
        StepVerifier.create(selected).expectNext("a", "a", "a").verifyComplete();
    }

    @Test
    void shouldPreserveOuterSourceErrorIdentity() {
        RuntimeException failure = new RuntimeException("source failed");
        StepVerifier.create(groups(2).apply(Flux.just("a", "b").concatWith(Flux.error(failure))))
                .expectNextMatches(group -> group instanceof GroupedFlux
                        && "a".equals(((GroupedFlux<?, ?>) group).key()))
                .expectNextMatches(group -> group instanceof GroupedFlux
                        && "b".equals(((GroupedFlux<?, ?>) group).key()))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }

    @Test
    void shouldPreserveDemandContextAndNonFusionBoundary() {
        Flux<String> group = groups(1).apply(Flux.just("a", "a")).single().block();
        Assertions.assertNotNull(group);
        Object marker = new Object();
        StepVerifier.create(group.contextWrite(context -> context.put("group-test", marker)), 0)
                .expectFusion(Fuseable.NONE)
                .expectAccessibleContext().contains("group-test", marker).then()
                .thenRequest(1).expectNext("a")
                .thenRequest(1).expectNext("a")
                .verifyComplete();
    }

    @Test
    void shouldPreserveSubscriptionDiagnostics() {
        Flux<String> group = groups(1).apply(Flux.just("a")).single().block();
        Assertions.assertNotNull(group);
        RecordingSubscriber<String> inner = new RecordingSubscriber<>();
        group.subscribe(inner);
        Scannable subscription = Scannable.from(inner.subscription);
        Assertions.assertNotNull(subscription.scan(Scannable.Attr.PARENT));
        Assertions.assertSame(inner, subscription.scanUnsafe(Scannable.Attr.ACTUAL));
        Assertions.assertEquals(Scannable.Attr.RunStyle.SYNC, subscription.scan(Scannable.Attr.RUN_STYLE));
        Assertions.assertEquals(0, subscription.scan(Scannable.Attr.BUFFERED));
        Assertions.assertFalse(subscription.scan(Scannable.Attr.TERMINATED));
        Assertions.assertFalse(subscription.scan(Scannable.Attr.CANCELLED));
        inner.subscription.request(1);
        Assertions.assertEquals(Collections.singletonList("a"), inner.values);
        Assertions.assertTrue(inner.completed);
        Assertions.assertTrue(subscription.scan(Scannable.Attr.TERMINATED));
        Assertions.assertTrue(subscription.scan(Scannable.Attr.CANCELLED));
    }

    @Test
    void shouldPreserveInnerAndOuterSourceErrorIdentity() {
        RuntimeException failure = new RuntimeException("source failed");
        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber<String> inner = new RecordingSubscriber<>();
        RecordingSubscriber<Flux<String>> outer = new RecordingSubscriber<>();
        groups(1).apply(source.flux()).doOnNext(group -> group.subscribe(inner)).subscribe(outer);
        outer.subscription.request(Long.MAX_VALUE);
        source.next("a");
        inner.subscription.request(1);
        source.error(failure);
        Assertions.assertEquals(Collections.singletonList("a"), inner.values);
        Assertions.assertSame(failure, inner.error);
        Assertions.assertSame(failure, outer.error);
        Assertions.assertFalse(inner.completed);
    }

    @Test
    void shouldPropagateBudgetErrorOnlyThroughOuterUntilInnerCancellation() {
        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber<String> inner = new RecordingSubscriber<>();
        RecordingSubscriber<Flux<String>> outer = new RecordingSubscriber<>();
        groups(1).apply(source.flux()).doOnNext(group -> group.subscribe(inner)).subscribe(outer);
        outer.subscription.request(Long.MAX_VALUE);
        source.next("a");
        inner.subscription.request(Long.MAX_VALUE);
        source.next("b");
        Assertions.assertNotNull(outer.error);
        Assertions.assertTrue(outer.error.getMessage().contains(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS));
        Assertions.assertEquals(Collections.singletonList("a"), inner.values);
        Assertions.assertNull(inner.error);
        Assertions.assertFalse(inner.completed);
        inner.subscription.request(1);
        Assertions.assertNull(inner.error);
        Assertions.assertFalse(inner.completed);
        inner.subscription.cancel();
        inner.subscription.cancel();
        source.assertCancelled();
    }

    @Test
    void shouldNotSuppressOrdinaryValueSelectorError() {
        RuntimeException failure = new RuntimeException("value selector failed");
        DefaultReactorQLMetadata metadata = metadata(2);
        Function<Flux<String>, Flux<Flux<String>>> mapper = GroupStateBudget.createGroupMapper(
                metadata, Function.identity(), value -> {
                    if ("b".equals(value)) {
                        throw failure;
                    }
                    return value;
                });
        RecordingSubscriber<String> inner = new RecordingSubscriber<>();
        RecordingSubscriber<Flux<String>> outer = new RecordingSubscriber<>();
        TestPublisher<String> source = TestPublisher.create();
        mapper.apply(source.flux()).doOnNext(group -> group.subscribe(inner)).subscribe(outer);
        outer.subscription.request(Long.MAX_VALUE);
        source.next("a");
        inner.subscription.request(Long.MAX_VALUE);
        source.next("b");
        Assertions.assertEquals(Collections.singletonList("a"), inner.values);
        Assertions.assertSame(failure, inner.error);
        Assertions.assertSame(failure, outer.error);
    }

    @Test
    void shouldReleaseBufferedRowsOnlyWhenConsumedOrCancelled() {
        DefaultReactorQLMetadata metadata = metadata(2);
        metadata.setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 2);
        Function<Flux<String>, Flux<Flux<String>>> mapper = GroupStateBudget.createGroupMapper(
                metadata, Function.identity(), Function.identity());
        SubscriptionContext shared = new SubscriptionContext();
        Flux<String> group = mapper.apply(Flux.just("a", "a"))
                .contextWrite(context -> context.put(SubscriptionContext.class, shared)).single().block();
        Assertions.assertNotNull(group);
        RecordingSubscriber<String> inner = new RecordingSubscriber<>();
        group.subscribe(inner);
        Assertions.assertTrue(inner.values.isEmpty());
        StepVerifier.create(mapper.apply(Flux.just("b"))
                        .contextWrite(context -> context.put(SubscriptionContext.class, shared)))
                .expectErrorMatches(error -> error.getMessage().contains(
                        DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS)).verify();
        inner.subscription.request(1);
        Assertions.assertEquals(Collections.singletonList("a"), inner.values);
        verifyFreshGroup(mapper, shared, "b");
        inner.subscription.cancel();
        verifyFreshGroup(mapper, shared, "c");
    }

    @Test
    void shouldReleaseGroupOnceAcrossReentrantCompletionAndRepeatedCancellation() {
        Function<Flux<String>, Flux<Flux<String>>> mapper = groups(2);
        SubscriptionContext shared = new SubscriptionContext();
        List<Flux<String>> retained = mapper.apply(Flux.just("a", "b"))
                .contextWrite(context -> context.put(SubscriptionContext.class, shared)).collectList().block();
        Assertions.assertNotNull(retained);
        Assertions.assertEquals(2, retained.size());
        RecordingSubscriber<String> first = new RecordingSubscriber<>();
        first.onCompletion = () -> first.subscription.cancel();
        retained.get(0).subscribe(first);
        first.subscription.request(1);
        Assertions.assertTrue(first.completed);
        first.subscription.cancel();
        first.subscription.cancel();
        // The second group is still unconsumed: duplicate cleanup must not release its shared key budget.
        StepVerifier.create(mapper.apply(Flux.just("c"))
                        .contextWrite(context -> context.put(SubscriptionContext.class, shared)))
                .expectErrorMatches(error -> error.getMessage().contains(
                        DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS)).verify();
        RecordingSubscriber<String> second = new RecordingSubscriber<>();
        retained.get(1).subscribe(second);
        second.subscription.cancel();
        verifyFreshGroup(mapper, shared, "c");
    }

    @Test
    void shouldAllowImmediateInnerCancellationAndSubscriptionIsolation() {
        Function<Flux<String>, Flux<Flux<String>>> mapper = groups(1);
        SubscriptionContext shared = new SubscriptionContext();
        mapper.apply(Flux.just("a"))
                .contextWrite(context -> context.put(SubscriptionContext.class, shared))
                .doOnNext(group -> group.subscribe(new CoreSubscriber<String>() {
                    @Override
                    public void onSubscribe(Subscription subscription) {
                        subscription.cancel();
                    }

                    @Override
                    public void onNext(String value) {
                        Assertions.fail("Cancelled group must not emit");
                    }

                    @Override
                    public void onError(Throwable error) {
                        Assertions.fail(error);
                    }

                    @Override
                    public void onComplete() {
                        Assertions.fail("Cancelled group must not complete");
                    }
                })).then().block();
        verifyFreshGroup(mapper, shared, "b");
        StepVerifier.create(mapper.apply(Flux.just("a")).flatMap(Function.identity()))
                .expectNext("a").verifyComplete();
        StepVerifier.create(mapper.apply(Flux.just("b")).flatMap(Function.identity()))
                .expectNext("b").verifyComplete();
    }

    private void verifyFreshGroup(Function<Flux<String>, Flux<Flux<String>>> mapper,
                                  SubscriptionContext shared, String value) {
        StepVerifier.create(mapper.apply(Flux.just(value)).flatMap(Function.identity())
                        .contextWrite(context -> context.put(SubscriptionContext.class, shared)))
                .expectNext(value).verifyComplete();
    }

    private static final class RecordingSubscriber<T> implements CoreSubscriber<T> {

        private final List<T> values = new ArrayList<>();
        private Subscription subscription;
        private Throwable error;
        private boolean completed;
        private Runnable onCompletion;

        @Override
        public Context currentContext() {
            return Context.empty();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onComplete() {
            completed = true;
            if (onCompletion != null) {
                onCompletion.run();
            }
        }
    }

    private Function<Flux<String>, Flux<Flux<String>>> groups(int maxKeys) {
        return GroupStateBudget.createGroupMapper(metadata(maxKeys), Function.identity(), Function.identity());
    }

    private DefaultReactorQLMetadata metadata(int maxKeys) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select count(1) from test group by key");
        metadata.setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, maxKeys);
        return metadata;
    }
}
