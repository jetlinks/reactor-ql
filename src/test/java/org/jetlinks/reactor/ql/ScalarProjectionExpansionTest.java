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
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.FirstValueMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class ScalarProjectionExpansionTest {

    @Test
    void singlePublisherColumnKeepsLazyEntryRecoveryAndEmptyRows() {
        for (boolean empty : Arrays.asList(false, true)) {
            AtomicInteger calls = new AtomicInteger();
            RuntimeException failure = new IllegalStateException("mapper construction");
            ReactorQL query = ReactorQL.builder()
                    .feature(asyncFeature("one_value", record -> {
                        calls.incrementAndGet();
                        if (Integer.valueOf(1).equals(record.getRecord())) {
                            throw failure;
                        }
                        return Mono.deferContextual(context -> empty ? Mono.empty()
                                : Mono.just(context.get("single-column")));
                    }))
                    .sql("select one_value() a from test").build();
            Flux<Map<String, Object>> result = query.start(Flux.just(1, 2))
                    .onErrorContinue((error, value) -> {
                        Assertions.assertSame(failure, error);
                        Assertions.assertEquals("a", ((Map.Entry<?, ?>) value).getKey());
                    })
                    .contextWrite(context -> context.put("single-column", "visible"));
            Assertions.assertEquals(0, calls.get());
            for (int subscription = 1; subscription <= 2; subscription++) {
                StepVerifier.create(result)
                        .expectNext(Collections.emptyMap())
                        .expectNext(empty ? Collections.emptyMap() : Collections.singletonMap("a", "visible"))
                        .verifyComplete();
                Assertions.assertEquals(subscription * 2, calls.get());
            }
        }
    }

    @Test
    void mixedMapperFailureKeepsIdentityEntryDataAndSingleEvaluation() {
        for (Throwable failure : Arrays.asList(new IllegalStateException("mapper"), new AssertionError("mapper"))) {
            AtomicInteger invoked = new AtomicInteger();
            AtomicInteger sibling = new AtomicInteger();
            List<Object> hooked = new ArrayList<>();
            ReactorQL query = ReactorQL.builder()
                    .feature(scalarFeature("broken", record -> {
                        invoked.incrementAndGet();
                        if (failure instanceof Error) { throw (Error) failure; }
                        throw (RuntimeException) failure;
                    }))
                    .feature(asyncFeature("sibling", record -> {
                        sibling.incrementAndGet();
                        return Mono.just("survives");
                    }))
                    .sql("select broken() broken,sibling() good from test").build();
            Hooks.onOperatorError("mixed-mapper-data", (error, value) -> {
                Assertions.assertSame(failure, error);
                hooked.add(value);
                return error;
            });
            try {
                StepVerifier.create(query.start(Flux.just(1)))
                        .expectErrorMatches(error -> error == failure).verify();
                Assertions.assertEquals(1, invoked.get());
                Assertions.assertEquals(1, sibling.get());
                Assertions.assertEquals(1, hooked.size());
                Assertions.assertEquals("broken", ((Map.Entry<?, ?>) hooked.get(0)).getKey());
            } finally {
                Hooks.resetOnOperatorError("mixed-mapper-data");
            }
        }
    }

    @Test
    void publisherValueHookKeepsCallableFailureBoundary() {
        RuntimeException failure = new IllegalStateException("hooked callable");
        Mono<Object> valueSource = Mono.fromCallable(() -> { throw failure; });
        ReactorQL query = ReactorQL.builder()
                .feature(asyncFeature("publisher_value", record -> Mono.just(7)))
                .feature(asyncFeature("sibling", record -> Mono.just("survives")))
                .sql("select publisher_value() broken,sibling() good from test").build();
        List<Object> hooked = new ArrayList<>();
        Hooks.onEachOperator("scalar-callable", publisher -> {
            if (publisher instanceof reactor.core.Fuseable.ScalarCallable) {
                try {
                    if (Integer.valueOf(7).equals(((reactor.core.Fuseable.ScalarCallable<?>) publisher).call())) {
                        return valueSource;
                    }
                } catch (Exception error) { throw new AssertionError(error); }
            }
            return publisher;
        });
        Hooks.onOperatorError("scalar-callable", (error, value) -> {
            Assertions.assertSame(failure, error);
            hooked.add(value);
            return error;
        });
        try {
            StepVerifier.create(query.start(Flux.just(1)).onErrorContinue((error, value) -> {
                Assertions.assertSame(failure, error);
                Assertions.assertNull(value);
            })).verifyComplete();
            assertPublisherErrorData(valueSource, hooked);
        } finally {
            Hooks.resetOnEachOperator("scalar-callable");
            Hooks.resetOnOperatorError("scalar-callable");
        }
    }

    @Test
    void projectionCallableFailureKeepsValuePublisherAsOperatorErrorData() {
        for (boolean resume : Arrays.asList(false, true)) {
            RuntimeException failure = new RuntimeException("callable value");
            Mono<Object> valueSource = Mono.fromCallable(() -> { throw failure; });
            List<Object> hooked = new ArrayList<>();
            List<Object> continued = new ArrayList<>();
            Hooks.onOperatorError("projection-callable-data", (error, value) -> {
                Assertions.assertSame(failure, error);
                hooked.add(value);
                return error;
            });
            try {
                ReactorQL query = ReactorQL.builder()
                        .feature(asyncFeature("broken_callable", record -> valueSource))
                        .sql("select broken_callable() a,'sibling' b from test").build();
                Flux<Map<String, Object>> result = query.start(Flux.just(1));
                if (resume) {
                    result = result.onErrorContinue((error, value) -> {
                        Assertions.assertSame(failure, error);
                        continued.add(value);
                    });
                    StepVerifier.create(result).verifyComplete();
                    Assertions.assertEquals(Collections.singletonList(null), continued);
                } else {
                    StepVerifier.create(result).expectErrorMatches(error -> error == failure).verify();
                    Assertions.assertTrue(continued.isEmpty());
                }
                assertPublisherErrorData(valueSource, hooked);
            } finally {
                Hooks.resetOnOperatorError("projection-callable-data");
            }
        }
    }

    @Test
    void mixedProjectionDelaysAndCombinesFailuresInColumnOrder() {
        TestPublisher<String> left = TestPublisher.create();
        TestPublisher<String> right = TestPublisher.create();
        RuntimeException first = new RuntimeException("left");
        RuntimeException second = new RuntimeException("right");
        ReactorQL query = ReactorQL.builder()
                .feature(asyncFeature("left_value", record -> left.flux()))
                .feature(asyncFeature("right_value", record -> right.flux()))
                .sql("select left_value() a,right_value() b from test").build();
        StepVerifier.create(query.start(Flux.just(1)))
                .then(() -> right.error(second))
                .then(() -> left.error(first))
                .expectErrorSatisfies(error -> Assertions.assertEquals(
                        Arrays.asList(first, second), Exceptions.unwrapMultipleExcludingTracebacks(error)))
                .verify();
    }

    @Test
    void projectionFirstValuesKeepCancellationAndDiscardObservations() {
        for (int columns : Arrays.asList(1, 2)) {
            AtomicInteger cancelled = new AtomicInteger();
            List<Object> discarded = new ArrayList<>();
            ReactorQL query = ReactorQL.builder()
                    .feature(asyncFeature("many", record -> Flux.just("first", "second")
                            .concatWith(Flux.never()).doOnCancel(cancelled::incrementAndGet)))
                    .sql(columns == 1 ? "select many() a from test" : "select many() a,many() b from test")
                    .build();
            StepVerifier.create(query.start(Flux.just(1)).doOnDiscard(Object.class, discarded::add))
                    .expectNext(columns == 1 ? Collections.singletonMap("a", "first")
                            : TestRows.row("a", "first", "b", "first"))
                    .verifyComplete();
            Assertions.assertEquals(columns, cancelled.get());
            Assertions.assertEquals(columns == 1 ? Collections.singletonList("first")
                    : Collections.emptyList(), discarded);
        }
    }

    @Test
    void mixedScalarColumnsKeepEntryRecoveryNullAndAliasOrder() {
        for (boolean checkpoint : Arrays.asList(false, true)) {
            RuntimeException failure = new RuntimeException("scalar mapper");
            List<String> continued = new ArrayList<>();
            ReactorQL query = ReactorQL.builder().setting("checkpoint", checkpoint)
                    .feature(scalarFeature("broken", record -> { throw failure; }))
                    .feature(scalarFeature("empty_value", record -> null))
                    .feature(scalarFeature("read_alias", record -> record.getResultValue("first")))
                    .feature(asyncFeature("async", record -> Mono.just("async")))
                    .sql("select 'seed' first,broken() broken,empty_value() omitted,read_alias() copied,async() last from test")
                    .build();
            StepVerifier.create(query.start(Flux.just(1)).onErrorContinue((error, value) -> {
                Assertions.assertSame(failure, error);
                Assertions.assertTrue(value instanceof Map.Entry);
                continued.add((String) ((Map.Entry<?, ?>) value).getKey());
            }))
                    .expectNext(TestRows.row("first", "seed", "copied", "seed", "last", "async"))
                    .verifyComplete();
            Assertions.assertEquals(Collections.singletonList("broken"), continued);
        }
    }

    @Test
    void mixedScalarSetterFailureKeepsValueRecoveryBoundary() {
        RuntimeException failure = new RuntimeException("result setter");
        AtomicInteger continued = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.just(1)) {
            @Override
            public Map<String, Object> newContainer() {
                return new LinkedHashMap<String, Object>() {
                    @Override
                    public Object put(String key, Object value) {
                        if ("broken".equals(key)) { throw failure; }
                        return super.put(key, value);
                    }
                };
            }
        };
        ReactorQL query = ReactorQL.builder()
                .feature(scalarFeature("scalar", record -> "write-me"))
                .feature(asyncFeature("async", record -> Mono.just("survives")))
                .sql("select scalar() broken,async() good from test").build();
        StepVerifier.create(query.start(context).map(ReactorQLRecord::asMap).onErrorContinue((error, value) -> {
            Assertions.assertSame(failure, error);
            Assertions.assertEquals("write-me", value);
            continued.incrementAndGet();
        })).expectNext(Collections.singletonMap("good", "survives")).verifyComplete();
        Assertions.assertEquals(1, continued.get());
    }

    @Test
    void mixedProjectionKeepsPendingSubscriptionsContextAndCancellation() {
        for (boolean cancel : Arrays.asList(false, true)) {
            TestPublisher<String> left = TestPublisher.create();
            TestPublisher<String> right = TestPublisher.create();
            ReactorQL query = ReactorQL.builder()
                    .feature(scalarFeature("scalar", record -> "scalar"))
                    .feature(asyncFeature("left_value", record -> Mono.deferContextual(context -> {
                        Assertions.assertEquals("visible", context.get("projection-marker"));
                        return left.mono();
                    })))
                    .feature(asyncFeature("right_value", record -> right.mono()))
                    .sql("select left_value() a,scalar() b,right_value() c from test").build();
            StepVerifier.Step<Map<String, Object>> verifier = StepVerifier.create(query.start(Flux.just(1))
                    .contextWrite(context -> context.put("projection-marker", "visible")))
                    .then(() -> {
                        left.assertWasSubscribed();
                        right.assertWasSubscribed();
                    });
            if (cancel) {
                verifier.thenCancel().verify();
                left.assertCancelled();
                right.assertCancelled();
            } else {
                verifier.then(() -> right.emit("right"))
                        .then(() -> left.emit("left"))
                        .expectNext(TestRows.row("a", "left", "b", "scalar", "c", "right"))
                        .verifyComplete();
            }
        }
    }

    @Test
    void mixedProjectionKeepsExplicitFirstValueMapperPrecedence() {
        class DualMapper implements ScalarValueMapper, FirstValueMapper {
            @Override
            public Object applyScalar(ReactorQLRecord record) { return "scalar"; }

            @Override
            public Mono<?> first(ReactorQLRecord record) { return Mono.just("first"); }
        }
        ReactorQL query = ReactorQL.builder()
                .feature(asyncFeature("dual", new DualMapper()))
                .feature(asyncFeature("async", record -> Mono.just("async")))
                .sql("select dual() a,async() b from test").build();
        StepVerifier.create(query.start(Flux.just(1)))
                .expectNext(TestRows.row("a", "first", "b", "async"))
                .verifyComplete();
    }

    @Test
    void shouldKeepSynchronousCustomMapperOrderAndFailure() {
        List<String> invocations = new ArrayList<>();
        ValueMapFeature ordered = scalarFeature("ordered", record -> {
            invocations.add("ordered:" + record.getRecord());
            return invocations.size();
        });

        List<Map<String, Object>> rows = ReactorQL.builder()
                                                   .feature(ordered)
                                                   .sql("select ordered(this) as first,ordered(this) as second from test")
                                                   .build()
                                                   .start(Flux.just("a", "b"))
                                                   .collectList()
                                                   .block();

        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("ordered:a", "ordered:a", "ordered:b", "ordered:b")), invocations);
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList(1, 2)), Collections.unmodifiableList(Arrays.asList(rows.get(0).get("first"), rows.get(0).get("second"))));
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList(3, 4)), Collections.unmodifiableList(Arrays.asList(rows.get(1).get("first"), rows.get(1).get("second"))));

        IllegalStateException failure = new IllegalStateException("scalar failure");
        ReactorQL.builder()
                 .feature(scalarFeature("broken", record -> {
                     throw failure;
                 }))
                 .sql("select broken(this) as value from test")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                 .verify();
    }

    @Test
    void shouldKeepStarExpansionAndFieldOverwriteOrder() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("value", "source");
        source.put("other", 2);
        ValueMapFeature computed = scalarFeature("computed", record -> "projection");

        ReactorQL.builder()
                 .feature(computed)
                 .sql("select computed(this) as value,* from test")
                 .build()
                 .start(Flux.just(source))
                 .as(StepVerifier::create)
                 .assertNext(result -> {
                     Assertions.assertEquals("source", result.get("value"));
                     Assertions.assertEquals(2, result.get("other"));
                     Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("value", "other")), new ArrayList<>(result.keySet()));
                 })
                 .verifyComplete();

        ReactorQL.builder()
                 .feature(computed)
                 .sql("select computed(this) as value,t.* from test t")
                 .build()
                 .start(Flux.just(source))
                 .as(StepVerifier::create)
                 .assertNext(result -> {
                     Assertions.assertEquals("source", result.get("value"));
                     Assertions.assertEquals(2, result.get("other"));
                     Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("value", "other")), new ArrayList<>(result.keySet()));
                 })
                 .verifyComplete();
    }

    @Test
    void shouldKeepMixedAsyncProjectionBehavior() {
        ValueMapFeature scalar = scalarFeature("scalar", record -> "scalar:" + record.getRecord());
        ValueMapFeature async = asyncFeature("async", record -> Mono.just("async:" + record.getRecord()));

        ReactorQL.builder()
                 .feature(scalar)
                 .feature(async)
                 .sql("select scalar(this) as first,async(this) as second from test")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(TestRows.row("first", "scalar:1", "second", "async:1"))
                 .verifyComplete();
    }

    @Test
    void shouldKeepWideMixedProjectionAndCustomResultContainer() {
        ValueMapFeature scalar = scalarFeature("scalar", record -> "scalar:" + record.getRecord());
        ValueMapFeature async = asyncFeature("async", record -> Mono.just("async:" + record.getRecord()));
        ValueMapFeature missing = asyncFeature("missing", record -> Mono.empty());
        ReactorQL ql = ReactorQL.builder()
                               .feature(scalar)
                               .feature(async)
                               .feature(missing)
                               .sql("select async(this) a,scalar(this) b,missing(this) omitted,"
                                       + "scalar(this) c,scalar(this) d,async(this) e from test")
                               .build();
        Map<String, Object> expected = TestRows.row("a", "async:7", "b", "scalar:7",
                                              "c", "scalar:7", "d", "scalar:7", "e", "async:7");

        ql.start(Flux.just(7))
          .as(StepVerifier::create)
          .expectNext(expected)
          .verifyComplete();

        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext customContext = new DefaultReactorQLContext(ignore -> Flux.just(7)) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new LinkedHashMap<>();
            }
        };
        ql.start(customContext)
          .map(ReactorQLRecord::asMap)
          .as(StepVerifier::create)
          .assertNext(result -> {
              Assertions.assertEquals(expected, result);
              Assertions.assertInstanceOf(LinkedHashMap.class, result);
          })
          .verifyComplete();
        Assertions.assertEquals(2, containers.get());
    }

    private static void assertPublisherErrorData(Publisher<?> source, List<Object> hooked) {
        Assertions.assertEquals(1, hooked.size());
        Object data = hooked.get(0);
        // Assembly instrumentation can decorate the value publisher supplied to the error hook.
        Assertions.assertTrue(data == source || Scannable.from(data).parents().anyMatch(parent -> parent == source),
                              "operator error data must retain the original value publisher");
    }

    private static ValueMapFeature scalarFeature(String name, Function<ReactorQLRecord, Object> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return (ScalarValueMapper) mapper::apply;
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }

    private static ValueMapFeature asyncFeature(String name,
                                                Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }
}
