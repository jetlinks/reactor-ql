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
package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.TestRows;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** Compatibility checks for common expressions, including their native evaluation boundaries. */
class CommonExpressionCompatibilityTest {

    @Test
    void mixedProjectionHonorsPublisherHooksIntroducingAsyncContextAndCancellation() {
        for (boolean cancel : Arrays.asList(false, true)) {
            TestPublisher<Integer> pending = TestPublisher.create();
            // Subscription effects belong to ordinary Publisher mappers. ScalarValueMapper's
            // authoritative applyScalar contract does not expose an internal value Publisher.
            ReactorQL query = ReactorQL.builder().feature(properties(name -> row -> Mono.just(7)))
                    .sql("select now() n,p0 a from test").build();
            Hooks.onEachOperator("mixed-column-async", publisher -> {
                if (publisher instanceof Fuseable.ScalarCallable) {
                    try {
                        if (Integer.valueOf(7).equals(((Fuseable.ScalarCallable<?>) publisher).call())) {
                            return Mono.deferContextual(context -> {
                                assertEquals("visible", context.get("mixed-marker"));
                                return pending.mono();
                            });
                        }
                    } catch (Exception error) { throw new AssertionError(error); }
                }
                return publisher;
            });
            try {
                StepVerifier.Step<Map<String, Object>> verifier = StepVerifier
                        .create(query.start(Flux.just(Collections.singletonMap("good", 7)))
                                .contextWrite(context -> context.put("mixed-marker", "visible")))
                        .then(pending::assertWasSubscribed);
                if (cancel) {
                    verifier.thenCancel().verify();
                    pending.assertCancelled();
                } else {
                    verifier.then(() -> pending.emit(8)).assertNext(result -> {
                        assertEquals(8, result.get("a"));
                        assertTrue(result.get("n") instanceof Long);
                    }).verifyComplete();
                }
            } finally {
                Hooks.resetOnEachOperator("mixed-column-async");
            }
        }
    }

    @Test
    void mixedProjectionKeepsValueAssemblyHooks() {
        ReactorQL query = ReactorQL.builder().feature(properties(name -> row -> Mono.just(7)))
                .sql("select now() n,p0 a from test").build();
        Hooks.onEachOperator("mixed-column-value", publisher -> {
            if (publisher instanceof Fuseable.ScalarCallable) {
                try {
                    if (Integer.valueOf(7).equals(((Fuseable.ScalarCallable<?>) publisher).call())) {
                        return Mono.just(8);
                    }
                } catch (Exception error) { throw new AssertionError(error); }
            }
            return publisher;
        });
        try {
            Map<String, Object> result = query.start(Flux.just(Collections.singletonMap("good", 7))).blockFirst();
            assertEquals(8, result.get("a"));
            assertTrue(result.get("n") instanceof Long);
        } finally {
            Hooks.resetOnEachOperator("mixed-column-value");
        }
    }

    @Test
    void mixedProjectionKeepsConcurrentAndRepeatedSubscriptionsIndependent() {
        ReactorQL query = ReactorQL.builder().sql("select now() n,this.good a,a+1 next from test").build();
        for (int repetition = 0; repetition < 2; repetition++) {
            List<Map<String, Object>> results = Flux.range(0, 64)
                    .flatMap(id -> query.start(Flux.just(Collections.singletonMap("good", id)))
                            .subscribeOn(Schedulers.parallel()))
                    .collectList().block();
            assertEquals(64, results.size());
            boolean[] seen = new boolean[64];
            for (Map<String, Object> result : results) {
                int id = ((Number) result.get("a")).intValue();
                assertFalse(seen[id]);
                seen[id] = true;
                assertEquals(id + 1, ((Number) result.get("next")).intValue());
                assertTrue(result.get("n") instanceof Long);
            }
        }
    }

    @Test
    void clockProjectionKeepsSiblingFailureRecovery() {
        for (boolean checkpoint : Arrays.asList(false, true)) {
            RuntimeException failure = new IllegalStateException("field read");
            Map<String, Object> input = new HashMap<String, Object>() {
                @Override
                public Object get(Object key) {
                    if ("broken".equals(key)) throw failure;
                    return super.get(key);
                }
            };
            input.put("good", 7);
            List<Object> recovered = new ArrayList<>();
            List<Map<String, Object>> rows = ReactorQL.builder().setting("checkpoint", checkpoint)
                    .sql("select now() n,good a,broken b from test").build()
                    .start(Flux.just(input)).onErrorContinue((error, value) -> {
                        assertSame(failure, error);
                        recovered.add(value);
                    }).collectList().block();
            assertEquals(1, rows.size());
            assertEquals(7, rows.get(0).get("a"));
            assertTrue(rows.get(0).get("n") instanceof Long);
            assertFalse(rows.get(0).containsKey("b"));
            assertEquals(1, recovered.size());
            assertEquals("b", ((Map.Entry<?, ?>) recovered.get(0)).getKey());
        }
    }

    @Test
    void nestedClockStillSamplesWhenItsMapperIsInvoked() {
        Function<ReactorQLRecord, Publisher<?>> mapper = mapper("clock_identity(now())",
                new SingleParameterFunctionMapFeature("clock_identity", Function.identity()));
        Publisher<?> sampled = mapper.apply(record());
        long afterInvocation = System.currentTimeMillis();
        // Delayed subscription distinguishes eager clock sampling from a new Supplier boundary.
        StepVerifier.create(Mono.delay(Duration.ofMillis(50)).then(Mono.from(sampled)))
                .assertNext(value -> assertTrue((Long) value <= afterInvocation))
                .verifyComplete();
    }

    @Test
    void coalesceBuildsAllCandidatesInOrderAndKeepsTheSelectedStream() {
        List<String> constructed = new ArrayList<>();
        List<String> subscribed = new ArrayList<>();
        ValueMapFeature parameters = properties(name -> row -> {
            constructed.add(name);
            return Flux.deferContextual(context -> {
                assertEquals("kept", context.get("expression-test"));
                subscribed.add(name);
                return "p2".equals(name) ? Flux.just(10, 11) : Flux.empty();
            });
        });
        Publisher<?> result = mapper("coalesce(p0,p1,p2,p3)", parameters).apply(record());
        assertEquals(Arrays.asList("p0", "p1", "p2", "p3"), constructed);
        assertTrue(subscribed.isEmpty());
        StepVerifier.create(Flux.<Object>from(result).contextWrite(Context.of("expression-test", "kept")), 0)
                .thenRequest(1).expectNext(10).thenRequest(1).expectNext(11).verifyComplete();
        assertEquals(Arrays.asList("p0", "p1", "p2"), subscribed);
    }

    @Test
    void coalesceKeepsUnusedConstructionFailuresAndEmptyFallback() {
        RuntimeException failure = new IllegalArgumentException("construction");
        ValueMapFeature parameters = properties(name -> row -> {
            if ("p3".equals(name)) throw failure;
            return Mono.just(1);
        });
        Function<ReactorQLRecord, Publisher<?>> mapper = mapper("coalesce(p0,p1,p2,p3)", parameters);
        assertSame(failure, assertThrows(RuntimeException.class, () -> mapper.apply(record())));
        StepVerifier.create(Flux.from(mapper("coalesce(null,null,null,null)").apply(record())))
                .verifyComplete();
        StepVerifier.create(Flux.<Object>from(mapper("coalesce(null,null,null,4)").apply(record())))
                .expectNext(4L).verifyComplete();
    }

    @Test
    void scalarCoalesceKeepsEagerOrderedReadsAndSubscriptionReuse() {
        List<String> read = new ArrayList<>();
        ValueMapFeature parameters = properties(name -> (ScalarValueMapper) row -> {
            read.add(name);
            return "p0".equals(name) ? 7 : 8;
        });
        Publisher<?> selected = mapper("coalesce(p0,p1,p2)", parameters).apply(record());
        assertEquals(Arrays.asList("p0", "p1", "p2"), read);
        for (int subscription = 0; subscription < 2; subscription++) {
            StepVerifier.create(Flux.<Object>from(selected)).expectNext(7).verifyComplete();
        }
        assertEquals(Arrays.asList("p0", "p1", "p2"), read);
    }

    @Test
    void scalarCoalesceUnusedFailureKeepsIndependentColumnsAndEntryRecovery() {
        RuntimeException failure = new IllegalStateException("unused fallback");
        List<String> read = new ArrayList<>();
        List<Object> recovered = new ArrayList<>();
        ValueMapFeature parameters = properties(name -> (ScalarValueMapper) row -> {
            read.add(name);
            if ("p1".equals(name)) throw failure;
            return 7;
        });
        StepVerifier.create(ReactorQL.builder().feature(parameters)
                .sql("select 'before' a,coalesce(p0,p1,p2) broken,'after' b from test").build()
                .start(Flux.just(1)).onErrorContinue((error, value) -> {
                    assertSame(failure, error); recovered.add(value);
                }))
                .expectNext(TestRows.row("a", "before", "b", "after")).verifyComplete();
        assertEquals(Arrays.asList("p0", "p1"), read);
        assertEquals(1, recovered.size());
        assertEquals("broken", ((Map.Entry<?, ?>) recovered.get(0)).getKey());
    }

    @Test
    void scalarCoalesceKeepsNestedFunctionsAndConcurrentSubscriptionsIndependent() {
        ReactorQL query = ReactorQL.builder()
                .sql("select upper(trim(coalesce(this.a,this.b,' fallback '))) v from test").build();
        StepVerifier.create(query.start(Flux.just(Collections.emptyMap())))
                .expectNext(Collections.singletonMap("v", "FALLBACK")).verifyComplete();
        StepVerifier.create(Flux.range(0, 64).flatMap(id -> query
                .start(Flux.just(Collections.singletonMap(id % 2 == 0 ? "a" : "b", " value-" + id + " ")))
                .doOnNext(row -> assertEquals("VALUE-" + id, row.get("v")))
                .subscribeOn(Schedulers.parallel()), 16).count())
                .expectNext(64L).verifyComplete();
        StepVerifier.create(ReactorQL.builder()
                .sql("select count(coalesce(this.a,this.b,'fallback')) total from test").build()
                .start(Flux.just(Collections.singletonMap("a", "first"),
                        Collections.singletonMap("b", "second"), Collections.emptyMap())))
                .expectNext(Collections.singletonMap("total", 3L)).verifyComplete();
    }

    @Test
    void coalesceCancellationReachesOnlyTheActiveCandidate() {
        TestPublisher<Object> active = TestPublisher.create();
        AtomicInteger unused = new AtomicInteger();
        ValueMapFeature parameters = properties(name -> row -> {
            if ("p0".equals(name)) return Mono.empty();
            if ("p1".equals(name)) return active.flux();
            return Flux.defer(() -> { unused.incrementAndGet(); return Flux.just(1); });
        });
        StepVerifier.create(Flux.from(mapper("coalesce(p0,p1,p2,p3)", parameters).apply(record())), 0)
                .then(() -> active.assertSubscribers(1)).thenCancel().verify();
        active.assertCancelled();
        assertEquals(0, unused.get());
    }

    @Test
    void publisherCoalescePreservesNativeAssemblyHookComposition() {
        Hooks.onEachOperator("common-expression-fallback", publisher -> {
            if (publisher instanceof Flux && "switchIfEmpty".equals(Scannable.from(publisher).stepName())) {
                return ((Flux<Object>) publisher).map(value -> ((Number) value).longValue() + 1);
            }
            return publisher;
        });
        try {
            ValueMapFeature parameters = properties(name -> row -> Mono.just(Long.parseLong(name.substring(1)) + 1));
            StepVerifier.create(Flux.<Object>from(mapper("coalesce(p0,p1,p2,p3)", parameters).apply(record())))
                    .expectNext(4L).verifyComplete();
        } finally {
            Hooks.resetOnEachOperator("common-expression-fallback");
        }
    }

    @Test
    void coalesceDoesNotFallbackAfterTheSelectedSourceEmitsThenFails() {
        RuntimeException failure = new IllegalStateException("selected stream");
        AtomicInteger unused = new AtomicInteger();
        ValueMapFeature parameters = properties(name -> row -> {
            if ("p0".equals(name)) return Mono.empty();
            if ("p1".equals(name)) return Flux.just(1, 2).concatWith(Mono.error(failure));
            return Flux.defer(() -> { unused.incrementAndGet(); return Flux.just(3); });
        });
        StepVerifier.create(Flux.<Object>from(mapper("coalesce(p0,p1,p2,p3)", parameters).apply(record())), 0)
                .thenRequest(1).expectNext(1).thenRequest(1).expectNext(2)
                .expectErrorMatches(error -> error == failure).verify();
        assertEquals(0, unused.get());
    }

    @Test
    void caseKeepsAllMatchingBranchesAcrossQueueSizes() {
        for (int count : new int[]{1, 2, 3, 16, 257}) {
            StringBuilder expression = new StringBuilder("case");
            List<Object> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                expression.append(" when 1=1 then ").append(i);
                expected.add((long) i);
            }
            expression.append(" else -1 end");
            assertEquals(expected, Flux.from(mapper(expression.toString()).apply(record())).collectList().block());
        }
        StepVerifier.create(Flux.<Object>from(mapper("case when 1=1 then null when 1=1 then null else 7 end")
                .apply(record()))).expectNext(7L).verifyComplete();
    }

    @Test
    void caseKeepsDemandAndEmptyThenFallback() {
        Publisher<?> result = mapper("case when 1=1 then null when 1=1 then 10 when 1=1 then 20 else 30 end")
                .apply(record());
        StepVerifier.create(Flux.<Object>from(result), 0)
                .thenRequest(1).expectNext(10L)
                .thenRequest(1).expectNext(20L).verifyComplete();
        StepVerifier.create(Flux.<Object>from(mapper("case when 1=1 then null when 1=1 then null else 30 end")
                .apply(record())), 0).thenRequest(1).expectNext(30L).verifyComplete();
    }

    @Test
    void casePredicateFailuresRemainDelayedAfterFollowingBranchValues() {
        RuntimeException failure = new IllegalStateException("condition");
        ValueMapFeature parameter = properties(name -> (ScalarValueMapper) ctx -> { throw failure; });
        List<Object> recovered = new ArrayList<>();
        StepVerifier.create(Flux.<Object>from(mapper("case when broken=1 then 10 when 1=1 then 20 end", parameter)
                .apply(record())).onErrorContinue((error, value) -> recovered.add(value)))
                .expectNext(20L).expectErrorMatches(error -> error == failure).verify();
        assertTrue(recovered.isEmpty());
    }

    @Test
    void caseEvaluatesAsyncConditionsInOrderAndKeepsContext() {
        TestPublisher<Integer> first = TestPublisher.create();
        TestPublisher<Integer> second = TestPublisher.create();
        ValueMapFeature parameters = properties(name -> row -> Mono.deferContextual(context -> {
            assertEquals("kept", context.get("expression-test"));
            return ("p0".equals(name) ? first : second).mono();
        }));
        Publisher<?> result = mapper("case when p0=1 then 10 when p1=1 then 20 else 30 end", parameters)
                .apply(record());
        StepVerifier.create(Flux.<Object>from(result).contextWrite(Context.of("expression-test", "kept")), 0)
                .then(() -> { first.assertSubscribers(1); second.assertNoSubscribers(); })
                .then(() -> first.emit(0))
                .then(() -> second.assertSubscribers(1))
                .then(() -> second.emit(1))
                .thenRequest(1).expectNext(20L).verifyComplete();
    }

    @Test
    void caseCancellationStopsThePendingCondition() {
        TestPublisher<Integer> active = TestPublisher.create();
        ValueMapFeature parameters = properties(name -> row -> active.mono());
        StepVerifier.create(Flux.from(mapper("case when p0=1 then 10 when p1=1 then 20 end", parameters)
                .apply(record())), 0).then(() -> active.assertSubscribers(1)).thenCancel().verify();
        active.assertCancelled();
    }

    @Test
    void nestedUnaryKeepsMultiValueRecoveryAndHookData() {
        RuntimeException failure = new IllegalArgumentException("calculate");
        SingleParameterFunctionMapFeature convert = new SingleParameterFunctionMapFeature("convert_value", value -> {
            if ("bad".equals(value)) throw failure;
            return "converted:" + value;
        });
        ValueMapFeature parameter = properties(name -> row -> Flux.just("bad", "ok"));
        List<Object> recovered = new ArrayList<>();
        StepVerifier.create(Flux.<Object>from(mapper("upper(convert_value(p0))", convert, parameter).apply(record()))
                        .onErrorContinue((error, value) -> { assertSame(failure, error); recovered.add(value); }))
                .expectNext("CONVERTED:OK").verifyComplete();
        assertEquals(Collections.singletonList("bad"), recovered);

        List<Object> hooked = new ArrayList<>();
        Hooks.onOperatorError("common-expression", (error, value) -> {
            assertSame(failure, error); hooked.add(value); return error;
        });
        try {
            ValueMapFeature mono = properties(name -> row -> Mono.just("bad"));
            StepVerifier.create(Flux.from(mapper("upper(convert_value(p0))", convert, mono).apply(record())))
                    .expectErrorMatches(error -> error == failure).verify();
            assertEquals(Collections.singletonList("bad"), hooked);
        } finally {
            Hooks.resetOnOperatorError("common-expression");
        }
    }

    @Test
    void nestedUnaryKeepsMonoContextAndCancellation() {
        TestPublisher<String> active = TestPublisher.create();
        ValueMapFeature parameter = properties(name -> row -> Mono.deferContextual(context -> {
            assertEquals("kept", context.get("expression-test")); return active.mono();
        }));
        StepVerifier.create(Flux.from(mapper("upper(trim(p0))", parameter).apply(record()))
                .contextWrite(Context.of("expression-test", "kept")), 0)
                .then(() -> active.assertSubscribers(1)).thenCancel().verify();
        active.assertCancelled();
    }

    @Test
    void nestedUnaryProjectionStillCancelsTheMonoAfterItsFirstValue() {
        AtomicInteger cancelled = new AtomicInteger();
        ValueMapFeature parameter = properties(name -> row -> Mono.just(" value ")
                .doOnCancel(cancelled::incrementAndGet));
        StepVerifier.create(ReactorQL.builder().feature(parameter)
                .sql("select upper(trim(p0)) a from test").build().start(Flux.just(1)))
                .expectNext(Collections.singletonMap("a", "VALUE")).verifyComplete();
        assertEquals(1, cancelled.get());
    }

    @Test
    void fixedKeysKeepVirtualPropertiesAndDynamicSubclassLookup() {
        DefaultPropertyFeature feature = DefaultPropertyFeature.GLOBAL;
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1); map.put("`a`", 2);
        for (String key : Arrays.asList("a", "`a`", "``a``", "this", "$", "*", "size", "$size",
                "empty", "keys", "values", "entries", "0", "-1", "missing")) {
            Function<Object, Object> prepared = feature.preparePropertyValue(key);
            for (Object source : Arrays.asList(map, Arrays.asList(1, 2))) {
                Object expected = feature.getPropertyValue(key, source);
                Object actual = prepared.apply(source);
                if (expected instanceof Collection) {
                    assertEquals(new ArrayList<>((Collection<?>) expected), new ArrayList<>((Collection<?>) actual), key);
                } else {
                    assertEquals(expected, actual, key);
                }
            }
            assertNull(prepared.apply(null), key);
        }
        DefaultPropertyFeature extension = new DefaultPropertyFeature() {
            @Override
            public Object getPropertyValue(Object property, Object source) { return "extension:" + property; }
        };
        assertEquals("extension:a", extension.preparePropertyValue("a").apply(map));
    }

    @Test
    void fixedBracketKeysHonorAssemblyHookParameterChanges() {
        Map<String, Object> input = new HashMap<>();
        input.put("output", 1); input.put("other", 2);
        Function<ReactorQLRecord, Publisher<?>> mapper = mapper("this['output']");
        Hooks.onEachOperator("common-expression-key", publisher -> {
            if (publisher instanceof Fuseable.ScalarCallable) {
                try {
                    if ("output".equals(((Fuseable.ScalarCallable<?>) publisher).call())) return Mono.just("other");
                } catch (Exception error) { throw new AssertionError(error); }
            }
            return publisher;
        });
        try {
            ReactorQLRecord record = ReactorQLRecord.newRecord("test", input, ReactorQLContext.ofDatasource(t -> Flux.empty()));
            StepVerifier.create(Flux.<Object>from(mapper.apply(record))).expectNext(2).verifyComplete();
        } finally {
            Hooks.resetOnEachOperator("common-expression-key");
        }
    }

    @Test
    void mixedCoalesceCaseKeepsAsyncBranchCompletionOrderAndContext() {
        TestPublisher<Object> first = TestPublisher.create();
        TestPublisher<Object> slow = TestPublisher.create();
        List<String> subscribed = new ArrayList<>();
        ValueMapFeature parameters = properties(name -> row -> Flux.deferContextual(context -> {
            assertEquals("kept", context.get("expression-test"));
            subscribed.add(name);
            if ("p0".equals(name)) return first.flux();
            if ("p1".equals(name)) return slow.flux();
            return Flux.just(11);
        }));
        Function<ReactorQLRecord, Publisher<?>> expression = mapper(
                "coalesce(p0,case when 1=1 then p1 when 1=1 then p2 else -1 end)", parameters);
        StepVerifier.create(Flux.<Object>from(expression.apply(record()))
                        .contextWrite(Context.of("expression-test", "kept")), 0)
                .then(() -> assertEquals(Collections.singletonList("p0"), subscribed))
                .thenRequest(1).then(first::complete).expectNext(11)
                .then(() -> slow.assertSubscribers(1))
                .thenRequest(1).then(() -> slow.next(10).complete()).expectNext(10).verifyComplete();
        assertEquals(Arrays.asList("p0", "p1", "p2"), subscribed);
    }

    @Test
    void mixedCoalesceCaseFirstValueCancelsTheSlowerBranch() {
        TestPublisher<Object> first = TestPublisher.create();
        TestPublisher<Object> slow = TestPublisher.create();
        ValueMapFeature parameters = properties(name -> row -> {
            if ("p0".equals(name)) return first.flux();
            if ("p1".equals(name)) return slow.flux();
            return Mono.just(11);
        });
        Function<ReactorQLRecord, Publisher<?>> expression = mapper(
                "coalesce(p0,case when 1=1 then p1 when 1=1 then p2 else -1 end)", parameters);
        StepVerifier.create(Mono.<Object>from(expression.apply(record())), 0)
                .thenRequest(1).then(first::complete).expectNext(11).verifyComplete();
        slow.assertCancelled();
    }

    @Test
    void caseBranchDefinitionsKeepConcurrentRowsAndRepeatedSubscriptionsIndependent() {
        Function<ReactorQLRecord, Publisher<?>> expression = mapper(
                "case when id>=0 then id when id>=0 then id+1 else -1 end");
        StepVerifier.create(Flux.range(0, 128).flatMap(id -> {
            ReactorQLRecord row = ReactorQLRecord.newRecord("test", Collections.singletonMap("id", id),
                    ReactorQLContext.ofDatasource(table -> Flux.empty()));
            Publisher<?> result = expression.apply(row);
            List<Object> expected = Arrays.asList(id, id + 1L);
            return Flux.from(result).collectList().doOnNext(values -> assertEquals(expected, values))
                    .thenMany(Flux.from(result)).collectList()
                    .doOnNext(values -> assertEquals(expected, values))
                    .subscribeOn(Schedulers.parallel());
        }, 16).count()).expectNext(128L).verifyComplete();
    }

    private static Function<ReactorQLRecord, Publisher<?>> mapper(String expression, ValueMapFeature... features) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select " + expression + " a from test");
        for (ValueMapFeature feature : features) metadata.addFeature(feature);
        Expression ast = ((SelectItem<?>) metadata.getSql().getSelectItems().get(0)).getExpression();
        return ValueMapFeature.createMapperNow(ast, metadata);
    }

    private static ReactorQLRecord record() {
        return ReactorQLRecord.newRecord("test", Collections.emptyMap(), ReactorQLContext.ofDatasource(t -> Flux.empty()));
    }

    private static ValueMapFeature properties(Function<String, Function<ReactorQLRecord, Publisher<?>>> factory) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return factory.apply(((Column) expression).getColumnName());
            }

            @Override
            public String getId() { return FeatureId.ValueMap.property.getId(); }
        };
    }
}
