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
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class JsonPathAsyncParameterTest {

    @Test
    void shouldKeepColdArgumentsOrderedAndContextVisible() {
        List<String> subscriptions = new CopyOnWriteArrayList<>();
        ValueMapFeature document = cold("async_doc", record -> Mono.deferContextual(context -> {
            Assertions.assertEquals("visible", context.get("marker"));
            subscriptions.add("document");
            return Mono.just(((Map<?, ?>) record.getRecord()).get("json"));
        }));
        ValueMapFeature path = cold("async_path", record -> Mono.deferContextual(context -> {
            Assertions.assertEquals("visible", context.get("marker"));
            subscriptions.add("path");
            return Mono.just(((Map<?, ?>) record.getRecord()).get("path"));
        }));
        ReactorQL query = ReactorQL.builder()
                                   .feature(document, path)
                                   .sql("select json_get(async_doc(json), async_path(path)) lon from test")
                                   .build();
        Map<String, Object> row = row("{\"point\":{\"lon\":120.12}}", "$.point.lon", null);
        Flux<Map<String, Object>> result = query.start(Flux.just(row));
        Assertions.assertTrue(subscriptions.isEmpty());

        StepVerifier.create(result.contextWrite(context -> context.put("marker", "visible")), 0)
                    .thenRequest(1)
                    .assertNext(value -> {
                        Assertions.assertEquals(Collections.singleton("lon"), value.keySet());
                        Assertions.assertEquals(Double.valueOf(120.12), value.get("lon"));
                    })
                    .verifyComplete();
        Assertions.assertEquals(Arrays.asList("document", "path"), subscriptions);
    }

    @Test
    void shouldKeepEmptyErrorAndCancellationSignals() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        RuntimeException failure = new RuntimeException("document source failed");
        ValueMapFeature document = cold("async_doc", record -> Mono.defer(() -> {
            subscriptions.incrementAndGet();
            String mode = (String) ((Map<?, ?>) record.getRecord()).get("mode");
            if ("empty".equals(mode)) {
                return Mono.empty();
            }
            if ("error".equals(mode)) {
                return Mono.error(failure);
            }
            return Mono.never().doOnCancel(() -> cancelled.set(true));
        }));
        ReactorQL query = ReactorQL.builder()
                                   .feature(document)
                                   .sql("select json_get(async_doc(json), '$.point.lon') lon from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just(row(null, null, "empty"))))
                    .expectNext(Collections.emptyMap())
                    .verifyComplete();
        StepVerifier.create(query.start(Flux.just(row(null, null, "error"))))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
        StepVerifier.create(query.start(Flux.just(row(null, null, "never")).hide()), 1)
                    .then(() -> Assertions.assertEquals(3, subscriptions.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertEquals(3, subscriptions.get());
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldKeepFromDirectMultiValueAdapterBehavior() {
        List<Object> previous = Flux.from(Mono.fromDirect(Flux.just("first", "second"))
                                              .cast(Object.class))
                                    .collectList()
                                    .block();
        List<Object> current = Flux.from(Mono.<Object>fromDirect(Flux.just("first", "second")))
                                   .collectList()
                                   .block();
        Assertions.assertEquals(previous, current);
    }

    @Test
    void shouldKeepTwoParameterMultiValueAndEmptyOrder() {
        ValueMapFeature first = cold("async_first", record ->
                "first-empty".equals(record.getRecord()) ? Mono.empty() : Flux.just("a", "b"));
        ValueMapFeature second = cold("async_second", record ->
                "second-empty".equals(record.getRecord()) ? Mono.empty() : Flux.just("c", "d"));
        JsonPathFunctionMapFeature echo = new JsonPathFunctionMapFeature("json_args", 2, 2) {
            @Override
            protected Object evaluate(JsonFunctionContext context) {
                List<Object> values = new ArrayList<>();
                for (int i = 0; i < context.size(); i++) {
                    values.add(context.value(i));
                }
                return values;
            }
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(first, second, echo)
                                   .sql("select json_args(async_first(this), async_second(this)) args from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just("both", "first-empty", "second-empty")))
                    .expectNext(Collections.singletonMap("args", Arrays.asList("a", "b", "c", "d")))
                    .expectNext(Collections.singletonMap("args", Arrays.asList(null, "c", "d")))
                    .expectNext(Collections.singletonMap("args", Arrays.asList("a", "b", null)))
                    .verifyComplete();
    }

    @Test
    void shouldPropagateSecondParameterErrorAndCancellation() {
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        RuntimeException failure = new RuntimeException("second source failed");
        ValueMapFeature first = cold("async_first", record -> Mono.just("document"));
        ValueMapFeature second = cold("async_second", record ->
                "error".equals(record.getRecord())
                        ? Mono.error(failure)
                        : Mono.never()
                              .doOnSubscribe(subscription -> subscribed.set(true))
                              .doOnCancel(() -> cancelled.set(true)));
        JsonPathFunctionMapFeature echo = new JsonPathFunctionMapFeature("json_args", 2, 2) {
            @Override
            protected Object evaluate(JsonFunctionContext context) {
                return context.args();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(first, second, echo)
                                   .sql("select json_args(async_first(this), async_second(this)) args from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just("error")))
                    .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                    .verify();
        StepVerifier.create(query.start(Flux.just("never")).hide(), 1)
                    .then(() -> Assertions.assertTrue(subscribed.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    private static ValueMapFeature cold(String name,
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

    private static Map<String, Object> row(String json, String path, String mode) {
        Map<String, Object> value = new HashMap<>();
        value.put("json", json);
        value.put("path", path);
        value.put("mode", mode);
        return value;
    }
}
