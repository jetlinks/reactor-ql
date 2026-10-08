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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

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
        Assertions.assertEquals(1, containers.get());
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
