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

import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Compares collection value conversion against the real former Function Feature. */
class FirstValueCollectionFunctionTest {

    @Test
    void preservesValuesNullsOrderAndUnflattenedArrayIdentity() {
        Map<String, Object> emptyMap = Collections.emptyMap();
        Object[] array = {1, 2};
        List<Object> readings = Arrays.asList(value(null), value(7), Collections.emptyList(),
                                              Arrays.asList(3, 4), emptyMap, array, "text");
        for (boolean retained : Arrays.asList(true, false)) {
            Map<String, Object> result = query("rows_to_array(readings)", retained)
                    .start(name -> Flux.just(row("readings", readings))).single().block();
            List<?> actual = (List<?>) result.get("result");
            Assertions.assertEquals(Arrays.asList(7, 3, emptyMap, array, "text"), actual);
            Assertions.assertSame(emptyMap, actual.get(2));
            Assertions.assertSame(array, actual.get(3));
            Map<String, Object> source = row("first", emptyMap);
            source.put("last", value(null));
            StepVerifier.create(query("row_to_array(first,last)", retained).start(name -> Flux.just(source)))
                        .expectNext(Collections.singletonMap("result", Collections.singletonList(emptyMap)))
                        .verifyComplete();
        }
    }

    @Test
    void coldPublisherValuesPreserveContextAndMultipleEmissions() {
        for (boolean retained : Arrays.asList(true, false)) {
            AtomicInteger subscriptions = new AtomicInteger();
            Flux<Object> readings = Flux.deferContextual(context -> {
                subscriptions.incrementAndGet();
                return Flux.just(value(context.get("value")), value(null), value(9));
            });
            Flux<Map<String, Object>> result = query("rows_to_array(readings)", retained)
                    .start(name -> Flux.just(row("readings", readings)));
            Assertions.assertEquals(0, subscriptions.get());
            StepVerifier.create(result.contextWrite(context -> context.put("value", 7)))
                        .expectNext(Collections.singletonMap("result", Arrays.asList(7, 9)))
                        .verifyComplete();
            StepVerifier.create(result.contextWrite(context -> context.put("value", 8)))
                        .expectNext(Collections.singletonMap("result", Arrays.asList(8, 9)))
                        .verifyComplete();
            Assertions.assertEquals(2, subscriptions.get());
        }
    }

    @Test
    void activePublisherCancellationPropagatesOnce() {
        for (boolean retained : Arrays.asList(true, false)) {
            TestPublisher<Object> source = TestPublisher.create();
            AtomicInteger subscriptions = new AtomicInteger();
            AtomicInteger cancellations = new AtomicInteger();
            Flux<Object> readings = source.flux().doOnSubscribe(ignored -> subscriptions.incrementAndGet())
                                          .doOnCancel(cancellations::incrementAndGet);
            StepVerifier.create(query("rows_to_array(readings)", retained)
                    .start(name -> Flux.just(row("readings", readings))), 0)
                        .thenRequest(1)
                        .then(() -> {
                            Assertions.assertEquals(1, subscriptions.get());
                            source.next(value(7));
                        })
                        .thenCancel()
                        .verify();
            Assertions.assertEquals(1, cancellations.get());
            source.assertCancelled();
        }
    }

    @Test
    void asynchronousResultWaitsForCompletionAndDownstreamDemand() {
        for (boolean retained : Arrays.asList(true, false)) {
            TestPublisher<Object> source = TestPublisher.create();
            Flux<Map<String, Object>> result = query("rows_to_array(readings)", retained)
                    .start(name -> Flux.just(row("readings", source.flux())));
            StepVerifier.create(result, 0)
                        .then(() -> source.emit(value(7), value(null), value(9)))
                        .thenRequest(1)
                        .expectNext(Collections.singletonMap("result", Arrays.asList(7, 9)))
                        .verifyComplete();
        }
    }

    @Test
    void conversionErrorContinuationKeepsInputAndErrorIdentity() {
        for (boolean retained : Arrays.asList(true, false)) {
            RuntimeException failure = new IllegalArgumentException("first value failed");
            Map<String, Object> bad = failingValue(failure);
            List<Object> continued = new ArrayList<>();
            Flux<Map<String, Object>> result = query("rows_to_array(readings)", retained)
                    .start(name -> Flux.just(row("readings", Arrays.asList(value(1), bad, value(2)))))
                    .onErrorContinue((error, input) -> {
                        Assertions.assertSame(failure, error);
                        continued.add(input);
                    });
            StepVerifier.create(result)
                        .expectNext(Collections.singletonMap("result", Arrays.asList(1, 2)))
                        .verifyComplete();
            Assertions.assertEquals(1, continued.size());
            Assertions.assertSame(bad, continued.get(0));
        }
    }

    @Test
    void failedRecoveryAndOperatorErrorsKeepNativeScope() {
        for (boolean retained : Arrays.asList(true, false)) {
            RuntimeException failure = new IllegalArgumentException("first value failed");
            RuntimeException recovery = new IllegalStateException("recovery failed");
            Map<String, Object> bad = failingValue(failure);
            Flux<Map<String, Object>> source = query("rows_to_array(readings)", retained)
                    .start(name -> Flux.just(row("readings", Arrays.asList(bad, value(2)))));
            List<Throwable> recoveryErrors = new ArrayList<>();
            List<Object> recoveryInputs = new ArrayList<>();
            StepVerifier.create(source.onErrorContinue((error, input) -> {
                recoveryErrors.add(error);
                recoveryInputs.add(input);
                throw recovery;
            })).expectErrorMatches(error -> error == recovery).verify();
            // The native outer subscription also sees the failed recovery, with no row value.
            Assertions.assertEquals(Arrays.asList(failure, recovery), recoveryErrors);
            Assertions.assertEquals(2, recoveryInputs.size());
            Assertions.assertSame(bad, recoveryInputs.get(0));
            Assertions.assertNull(recoveryInputs.get(1));
            Assertions.assertTrue(Arrays.asList(recovery.getSuppressed()).contains(failure));

            List<Object> data = new ArrayList<>();
            Hooks.onOperatorError("first-value-collection", (error, input) -> {
                data.add(input);
                return error;
            });
            try {
                StepVerifier.create(source).expectErrorMatches(error -> error == failure).verify();
                Assertions.assertFalse(data.isEmpty());
                Assertions.assertSame(bad, data.get(0));
            } finally {
                Hooks.resetOnOperatorError("first-value-collection");
            }
        }
    }

    private static ReactorQL query(String expression, boolean retained) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                "select " + expression + " result from telemetry");
        if (retained) {
            metadata.addFeature(new FunctionMapFeature("row_to_array", 9999, 1, stream -> stream
                    .concatMap(v -> Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)), 0)
                    .collect(java.util.stream.Collectors.toList())));
            metadata.addFeature(new FunctionMapFeature("rows_to_array", 9999, 1, stream -> stream
                    .as(CastUtils::flatStream)
                    .concatMap(v -> Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)), 0)
                    .collect(java.util.stream.Collectors.toList())));
        }
        return new DefaultReactorQL(metadata);
    }

    private static Map<String, Object> value(Object value) {
        return Collections.singletonMap("value", value);
    }

    private static Map<String, Object> row(String name, Object value) {
        Map<String, Object> row = new HashMap<>();
        row.put(name, value);
        return row;
    }

    private static Map<String, Object> failingValue(RuntimeException failure) {
        return new AbstractMap<String, Object>() {
            @Override
            public int size() {
                throw failure;
            }

            @Override
            public Set<Entry<String, Object>> entrySet() {
                return Collections.emptySet();
            }
        };
    }
}
