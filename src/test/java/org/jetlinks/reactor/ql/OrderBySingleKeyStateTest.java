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

import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class OrderBySingleKeyStateTest {

    @Test
    void shouldKeepSingleKeyNullTopNOffsetAndGlobalOrderSemantics() {
        ReactorQL topN = ReactorQL.builder()
                                  .sql("select this.id id, this.rank rank from test "
                                               + "order by this.rank asc nulls last limit 1,3")
                                  .build();

        topN.start(Flux.just(row("id", "a", "rank", 2),
                            row("id", "b", "rank", 1),
                            row("id", "c", "rank", 1),
                            row("id", "d"),
                            row("id", "e", "rank", 3)))
            .map(value -> value.get("id"))
            .as(StepVerifier::create)
            .expectNext("b", "a", "e")
            .verifyComplete();

        ReactorQL global = ReactorQL.builder()
                                    .sql("select this.id id from test order by this.rank desc nulls first")
                                    .build();
        global.start(Flux.just(row("id", "null"), row("id", "two", "rank", 2), row("id", "one", "rank", 1)))
              .collectList()
              .as(StepVerifier::create)
              .assertNext(result -> {
                  Assertions.assertEquals("null", result.get(0).get("id"));
                  Assertions.assertEquals("two", result.get(1).get("id"));
                  Assertions.assertEquals("one", result.get(2).get("id"));
                  result.get(0).put("mutable", true);
                  Assertions.assertEquals(true, result.get(0).get("mutable"));
              })
              .verifyComplete();
    }

    @Test
    void shouldKeepAsyncSingleAndMultiKeySubscriptionsAndErrors() {
        AtomicInteger firstSubscriptions = new AtomicInteger();
        AtomicInteger secondSubscriptions = new AtomicInteger();
        ValueMapFeature first = key("single_key", record -> Mono.defer(() -> {
            firstSubscriptions.incrementAndGet();
            return Mono.just(((Map<?, ?>) record.getRecord()).get("first"));
        }));
        ValueMapFeature second = key("second_key", record -> Mono.defer(() -> {
            secondSubscriptions.incrementAndGet();
            return Mono.just(((Map<?, ?>) record.getRecord()).get("second"));
        }));
        Flux<Map<String, Object>> source = Flux.just(row("id", "b", "first", 2, "second", 1),
                                                     row("id", "a", "first", 1, "second", 2),
                                                     row("id", "c", "first", 1, "second", 1));

        ReactorQL.builder()
                 .feature(first)
                 .sql("select this.id id from test order by single_key(this) limit 2")
                 .build()
                 .start(source)
                 .map(value -> value.get("id"))
                 .as(StepVerifier::create)
                 .expectNext("a", "c")
                 .verifyComplete();
        Assertions.assertEquals(3, firstSubscriptions.get());

        ReactorQL.builder()
                 .feature(first, second)
                 .sql("select this.id id from test order by single_key(this), second_key(this) limit 3")
                 .build()
                 .start(source)
                 .map(value -> value.get("id"))
                 .as(StepVerifier::create)
                 .expectNext("c", "a", "b")
                 .verifyComplete();
        Assertions.assertEquals(6, firstSubscriptions.get());
        Assertions.assertEquals(3, secondSubscriptions.get());

        IllegalStateException failure = new IllegalStateException("single key failure");
        ReactorQL.builder()
                 .feature(key("failing_single_key", record -> Mono.error(failure)))
                 .sql("select this.id id from test order by failing_single_key(this) limit 1")
                 .build()
                 .start(Flux.just(row("id", "a")))
                 .as(StepVerifier::create)
                 .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                 .verify();
    }

    @Test
    void shouldKeepCustomScalarSingleKeyValuesNullsAndErrors() {
        AtomicInteger evaluations = new AtomicInteger();
        ScalarValueMapper scalar = record -> {
            evaluations.incrementAndGet();
            return ((Map<?, ?>) record.getRecord()).get("rank");
        };
        ValueMapFeature feature = key("custom_scalar_key", scalar);
        Flux<Map<String, Object>> source = Flux.just(row("id", "b", "rank", 2),
                                                     row("id", "null"),
                                                     row("id", "a", "rank", 1));

        ReactorQL.builder()
                 .feature(feature)
                 .sql("select this.id id from test order by custom_scalar_key(this) asc nulls last limit 2")
                 .build()
                 .start(source)
                 .map(value -> value.get("id"))
                 .as(StepVerifier::create)
                 .expectNext("a", "b")
                 .verifyComplete();

        ReactorQL.builder()
                 .feature(feature)
                 .sql("select this.id id from test order by custom_scalar_key(this) desc nulls first")
                 .build()
                 .start(source)
                 .map(value -> value.get("id"))
                 .as(StepVerifier::create)
                 .expectNext("null", "b", "a")
                 .verifyComplete();
        Assertions.assertEquals(6, evaluations.get());

        IllegalStateException failure = new IllegalStateException("custom scalar key failure");
        ScalarValueMapper failing = record -> {
            throw failure;
        };
        ReactorQL.builder()
                 .feature(key("failing_scalar_key", failing))
                 .sql("select this.id id from test order by failing_scalar_key(this) limit 1")
                 .build()
                 .start(Flux.just(row("id", "a")))
                 .as(StepVerifier::create)
                 .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                 .verify();
    }

    @Test
    void shouldEvaluateRejectedTopNCandidatesAndPropagateLateErrorsAndCancellation() {
        AtomicInteger evaluations = new AtomicInteger();
        ScalarValueMapper scalar = record -> {
            evaluations.incrementAndGet();
            return ((Map<?, ?>) record.getRecord()).get("rank");
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(key("observed_key", scalar))
                                   .sql("select this.id id from test "
                                                + "order by observed_key(this) asc nulls last limit 2")
                                   .build();

        query.start(Flux.just(row("id", "a", "rank", 1),
                              row("id", "b", "rank", 2),
                              row("id", "c", "rank", 3),
                              row("id", "d", "rank", 4),
                              row("id", "null")))
             .map(value -> value.get("id"))
             .as(StepVerifier::create)
             .expectNext("a", "b")
             .verifyComplete();
        Assertions.assertEquals(5, evaluations.get());

        IllegalStateException failure = new IllegalStateException("late key failure");
        ScalarValueMapper failing = record -> {
            if ("boom".equals(((Map<?, ?>) record.getRecord()).get("id"))) {
                throw failure;
            }
            return ((Map<?, ?>) record.getRecord()).get("rank");
        };
        ReactorQL failingQuery = ReactorQL.builder()
                                          .feature(key("late_key", failing))
                                          .sql("select this.id id from test order by late_key(this) limit 2")
                                          .build();
        StepVerifier.create(failingQuery.start(Flux.just(row("id", "a", "rank", 1),
                                                        row("id", "b", "rank", 2),
                                                        row("id", "boom", "rank", 3))))
                    .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                    .verify();

        AtomicBoolean cancelled = new AtomicBoolean();
        Flux<Map<String, Object>> open = Flux.concat(Flux.just(row("id", "a", "rank", 1)),
                                                     Flux.<Map<String, Object>>never())
                                             .doOnCancel(() -> cancelled.set(true));
        StepVerifier.create(query.start(open), 0).thenCancel().verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldKeepDescendingTopNContextAndDownstreamDemand() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select this.id id from test "
                                                + "order by this.rank desc nulls first limit 2")
                                   .build();
        Flux<Map<String, Object>> source = Flux.deferContextual(context -> {
            String tenant = context.get("tenant");
            return Flux.just(row("id", tenant + "-low", "rank", 1),
                             row("id", tenant + "-high", "rank", 3),
                             row("id", tenant + "-null"));
        });

        StepVerifier.create(query.start(source)
                                 .map(value -> value.get("id"))
                                 .contextWrite(Context.of("tenant", "acme")), 0)
                    .thenRequest(1)
                    .expectNext("acme-null")
                    .thenRequest(1)
                    .expectNext("acme-high")
                    .verifyComplete();
    }

    private static ValueMapFeature key(String id, Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    net.sf.jsqlparser.expression.Expression expression,
                    ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(id).getId();
            }
        };
    }

    private static Map<String, Object> row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put(String.valueOf(values[index]), values[index + 1]);
        }
        return row;
    }
}
