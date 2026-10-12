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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

class ReviewRegressionTest {

    private static final String FIRST_QUERY = "select (select id from lookup) v from main";
    private static final String LOOKUP = "lookup";
    private static final String VISIBLE = "visible";

    @Test
    void shouldCancelLargeSubqueryAfterFirstValueAndReuseOnlyThatValue() {
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL.builder()
                .sql(FIRST_QUERY)
                .build();

        query.start(name -> LOOKUP.equals(name)
                ? Flux.range(1, 10000).map(value -> row("id", value))
                      .doOnSubscribe(ignore -> subscriptions.incrementAndGet())
                      .doOnNext(ignore -> consumed.incrementAndGet())
                      .doOnCancel(() -> cancelled.set(true))
                : Flux.just(row("id", 0), row("id", 1)))
             .as(StepVerifier::create)
             .expectNext(row("v", row("id", 1)), row("v", row("id", 1)))
             .verifyComplete();
        Assertions.assertEquals(1, consumed.get());
        Assertions.assertEquals(1, subscriptions.get());
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldNotWaitForCompletionOrObserveErrorsAfterFirstSubqueryValue() {
        assertFirstValueTail(false);
        assertFirstValueTail(true);
    }

    private static void assertFirstValueTail(boolean errorTail) {
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL.builder().sql(FIRST_QUERY).build();
        query.start(name -> LOOKUP.equals(name)
                ? Flux.concat(Flux.just(row("id", 7)), errorTail
                        ? Flux.error(new IllegalStateException("after first"))
                        : Flux.never()).doOnCancel(() -> cancelled.set(true))
                : Flux.just(row("id", 0)))
             .as(StepVerifier::create)
             .expectNext(row("v", row("id", 7)))
             .expectComplete()
             .verify(Duration.ofSeconds(3));
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldNotApplyFullSnapshotLimitToFirstValueConsumption() {
        ReactorQL.builder().setting(DefaultReactorQL.SETTING_SUBQUERY_MAX_ROWS, 1)
                 .sql(FIRST_QUERY).build()
                 .start(name -> LOOKUP.equals(name)
                         ? Flux.range(1, 100).map(value -> row("id", value))
                         : Flux.just(row("id", 0)))
                 .as(StepVerifier::create)
                 .expectNext(row("v", row("id", 1)))
                 .verifyComplete();
    }

    @Test
    void shouldKeepFirstValueBoundaryInsideCast() {
        ReactorQL.builder()
                .sql("select cast((select id from lookup) as string) v from main")
                .build()
                .start(name -> LOOKUP.equals(name)
                        ? Flux.concat(Flux.just(row("id", 7)), Flux.never())
                        : Flux.just(row("id", 0)))
                .as(StepVerifier::create)
                .expectNext(row("v", "{id=7}"))
                .expectComplete()
                .verify(Duration.ofSeconds(3));
    }

    @Test
    void shouldPreserveFullMultiValueConsumptionAndSubscriptionIsolation() {
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                .sql("select this.id id from main where this.id in (select id from lookup)")
                .build();
        for (int subscription = 1; subscription <= 2; subscription++) {
            query.start(name -> LOOKUP.equals(name)
                    ? Flux.range(1, 3).map(value -> row("id", value))
                          .doOnSubscribe(ignore -> subscriptions.incrementAndGet())
                          .doOnNext(ignore -> consumed.incrementAndGet())
                    : Flux.just(row("id", 3), row("id", 1), row("id", 4)))
                 .as(StepVerifier::create)
                 .expectNext(row("id", 3), row("id", 1))
                 .verifyComplete();
            Assertions.assertEquals(subscription, subscriptions.get());
            Assertions.assertEquals(subscription * 3, consumed.get());
        }
    }

    @Test
    void shouldKeepExistsConsumptionAtFirstValue() {
        AtomicInteger consumed = new AtomicInteger();
        ReactorQL.builder().sql("select exists(select id from lookup) present from main").build()
                 .start(name -> LOOKUP.equals(name)
                         ? Flux.concat(Flux.just(row("id", 7)), Flux.never())
                               .doOnNext(ignore -> consumed.incrementAndGet())
                         : Flux.just(row("id", 0), row("id", 1)))
                 .as(StepVerifier::create)
                 .expectNext(row("present", true), row("present", true))
                 .expectComplete()
                 .verify(Duration.ofSeconds(3));
        Assertions.assertEquals(1, consumed.get());
    }

    @Test
    void shouldPreserveEmptyAndErrorSignalsBeforeFirstValue() {
        ReactorQL query = ReactorQL.builder().sql(FIRST_QUERY).build();
        query.start(name -> LOOKUP.equals(name) ? Flux.empty() : Flux.just(row("id", 0)))
             .as(StepVerifier::create)
             .expectNext(Collections.emptyMap())
             .verifyComplete();
        IllegalStateException failure = new IllegalStateException("before first");
        query.start(name -> LOOKUP.equals(name) ? Flux.error(failure) : Flux.just(row("id", 0)))
             .as(StepVerifier::create)
             .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
             .verify();
    }

    @Test
    void shouldCancelPendingFirstValueAndRetainSubscriberContext() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger subscriptions = new AtomicInteger();
        ReactorQL query = ReactorQL.builder().sql(FIRST_QUERY).build();
        Function<String, Publisher<?>> source = name -> LOOKUP.equals(name)
                ? Flux.deferContextual(context -> {
                    Assertions.assertEquals(VISIBLE, context.get("marker"));
                    subscriptions.incrementAndGet();
                    return Flux.never().doOnCancel(() -> cancelled.set(true));
                })
                : Flux.just(row("id", 0));
        StepVerifier.create(query.start(source).contextWrite(context -> context.put("marker", VISIBLE)), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldNotShareNamedRecordFallbackAcrossOuterRows() {
        for (String column : new String[]{"t", "lookup.t"}) {
            ReactorQL query = ReactorQL.builder()
                    .sql("select this.id id,(select " + column + " v from lookup) cached from main t")
                    .build();
            query.start(name -> LOOKUP.equals(name)
                    ? Flux.just(row("other", 0))
                    : Flux.just(row("id", 1), row("id", 2)))
                 .as(StepVerifier::create)
                 .expectNext(TestRows.row("id", 1, "cached", row("v", row("id", 1))),
                             TestRows.row("id", 2, "cached", row("v", row("id", 2))))
                 .verifyComplete();
        }
    }

    @Test
    void shouldExposeCompletedProjectionAliasesToFollowingColumns() {
        String[] sqls = {
                "select concat('x','y') a,a b from test",
                "select concat('x','y') a,concat(a,'z') b from test",
                "select cast('1' as double) a,cast(a as double) b from test"
        };
        Object[] first = {"xy", "xy", 1.0D};
        Object[] second = {"xy", "xyz", 1.0D};
        for (int index = 0; index < sqls.length; index++) {
            ReactorQL.builder().sql(sqls[index]).build().start(Flux.just(1))
                     .as(StepVerifier::create)
                     .expectNext(TestRows.row("a", first[index], "b", second[index]))
                     .verifyComplete();
        }
    }

    @Test
    void shouldSubscribeConcurrentProjectionColumnsWithoutDelayingEachWrite() {
        Sinks.One<Integer> first = Sinks.one();
        Sinks.One<Integer> second = Sinks.one();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicReference<ReactorQLRecord> input = new AtomicReference<>();
        ReactorQL query = ReactorQL.builder()
                .feature(feature("first_value", record -> {
                    input.set(record);
                    return first.asMono().doOnSubscribe(ignore -> subscriptions.incrementAndGet());
                }))
                .feature(feature("second_value", record -> second.asMono()
                        .doOnSubscribe(ignore -> subscriptions.incrementAndGet())))
                .sql("select first_value(this) a,second_value(this) b from test")
                .build();
        StepVerifier.create(query.start(Flux.just(1)), 0)
                    .thenRequest(1)
                    .then(() -> Assertions.assertEquals(2, subscriptions.get()))
                    .then(() -> Assertions.assertEquals(Sinks.EmitResult.OK, first.tryEmitValue(11)))
                    .then(() -> Assertions.assertEquals(row("a", 11), input.get().asMap()))
                    .then(() -> Assertions.assertEquals(Sinks.EmitResult.OK, second.tryEmitValue(22)))
                    .expectNext(TestRows.row("a", 11, "b", 22))
                    .verifyComplete();
        Assertions.assertInstanceOf(java.util.concurrent.ConcurrentHashMap.class, input.get().asMap());
    }

    @Test
    void shouldKeepDelayedProjectionErrorsAndEmptyColumns() {
        IllegalStateException first = new IllegalStateException("first");
        IllegalArgumentException second = new IllegalArgumentException("second");
        ReactorQL.builder()
                .feature(feature("broken_first", record -> Mono.error(first)))
                .feature(feature("broken_second", record -> Mono.error(second)))
                .sql("select broken_first(this) a,broken_second(this) b from test")
                .build().start(Flux.just(1))
                .as(StepVerifier::create)
                .expectErrorSatisfies(error -> {
                    List<Throwable> errors = Exceptions.unwrapMultipleExcludingTracebacks(error);
                    Assertions.assertEquals(2, errors.size());
                    Assertions.assertTrue(errors.contains(first));
                    Assertions.assertTrue(errors.contains(second));
                })
                .verify();
        ReactorQL.builder().feature(feature("missing", record -> Mono.empty()))
                 .sql("select missing(this) omitted,concat('x','y') value from test")
                 .build().start(Flux.just(1)).as(StepVerifier::create)
                 .expectNext(row("value", "xy")).verifyComplete();
    }

    @Test
    void shouldRetainOriginalProjectionEntryAtErrorRecoveryBoundary() {
        IllegalStateException failure = new IllegalStateException("mapper failed");
        List<Object> values = new ArrayList<>();
        Hooks.onOperatorError("review-projection-source", (error, value) -> {
            Assertions.assertSame(failure, error);
            values.add(value);
            return error;
        });
        try {
            ReactorQL.builder().feature(feature("broken", record -> {
                throw failure;
            })).sql("select broken(this) bad,concat('x','y') good from test")
                     .build().start(Flux.just(1))
                     .onErrorContinue((error, value) -> {
                         Assertions.assertSame(failure, error);
                         Map.Entry<?, ?> entry = Assertions.assertInstanceOf(Map.Entry.class, value);
                         Assertions.assertEquals("bad", entry.getKey());
                     })
                     .as(StepVerifier::create)
                     .expectNext(row("good", "xy"))
                     .verifyComplete();
            // onErrorContinue intercepts this mapper failure before the operator-error Hook.
            Assertions.assertTrue(values.isEmpty());
        } finally {
            Hooks.resetOnOperatorError("review-projection-source");
        }
    }

    @Test
    void shouldHonorLegacyRecordGetterWithoutRecursiveDelegation() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        DefaultReactorQLRecord record = new DefaultReactorQLRecord("test", row("id", 1), context) {
            @Override
            public Optional<Object> getRecord(String name) {
                return "virtual".equals(name) ? Optional.of(42) : super.getRecord(name);
            }
        };
        ReactorQL.builder().sql("select virtual val from test").build().start(Flux.just(record))
                 .as(StepVerifier::create)
                 .expectNext(row("val", 42))
                 .verifyComplete();
        Assertions.assertEquals(42, record.getRecordValue("virtual"));
        Assertions.assertEquals(row("id", 1), record.getRecordValue("test"));
        Assertions.assertNull(record.getRecordValue("missing"));
    }

    @Test
    void shouldCopyAndBindPublishedSourceViewRatherThanPrivateState() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        DefaultReactorQLRecord source = new DefaultReactorQLRecord("privateAlias", 9, context) {
            @Override
            public Map<String, Object> getRecords(boolean all) {
                return row(VISIBLE, 7);
            }
        };
        DefaultReactorQLRecord target = new DefaultReactorQLRecord(null, 0, context);
        target.addNamedRecords(source);
        Assertions.assertEquals(row(VISIBLE, 7), target.getRecords(false));
        source.bindNamedRecords(context);
        Assertions.assertEquals(row(VISIBLE, 7), context.getParameters());
    }

    @Test
    void shouldHonorContextBulkBindingOverride() {
        AtomicInteger calls = new AtomicInteger();
        DefaultReactorQLContext target = new DefaultReactorQLContext(ignore -> Flux.empty()) {
            @Override
            public ReactorQLContext bindAll(Map<String, Object> values) {
                calls.incrementAndGet();
                Assertions.assertEquals(row(VISIBLE, 7), values);
                return super.bindAll(values);
            }
        };
        DefaultReactorQLRecord source = new DefaultReactorQLRecord(VISIBLE, 7, target);
        Assertions.assertSame(target, source.bindNamedRecords(target));
        Assertions.assertEquals(1, calls.get());
        Assertions.assertEquals(row(VISIBLE, 7), target.getParameters());
    }

    @Test
    void shouldHonorTargetRecordBulkCopyOverride() {
        AtomicInteger copies = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        DefaultReactorQLRecord target = new DefaultReactorQLRecord(null, 0, context) {
            @Override
            public ReactorQLRecord addRecords(Map<String, Object> records) {
                copies.incrementAndGet();
                return super.addRecords(records);
            }
        };
        target.addNamedRecords(new DefaultReactorQLRecord(VISIBLE, 7, context));
        Assertions.assertEquals(1, copies.get());
        Assertions.assertEquals(row(VISIBLE, 7), target.getRecords(false));
    }

    private static ValueMapFeature feature(String name, Function<ReactorQLRecord, Publisher<?>> mapper) {
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

    private static Map<String, Object> row(String key, Object value) {
        return Collections.singletonMap(key, value);
    }
}
