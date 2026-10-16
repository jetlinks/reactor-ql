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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.jetlinks.reactor.ql.DefaultReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/** Compares parameter adaptation with the retained, real concatMap argument stream. */
class FunctionParameterStreamTest {

    @Test
    void argumentErrorsDefaultsAndRecoveryKeepTheSameMapperInput() {
        RuntimeException failure = new IllegalArgumentException("argument failure");
        ScalarValueMapper first = record -> {
            if ("bad".equals(record.getRecord())) {
                throw failure;
            }
            return "missing".equals(record.getRecord()) ? null : "first";
        };
        for (int count : Arrays.asList(2, 3)) {
            List<Function<ReactorQLRecord, Publisher<Object>>> arguments = arguments(
                    first, ScalarValueMapper.constant("tail"), count == 3 ? ScalarValueMapper.constant(null) : null);
            for (Object fallback : Arrays.asList(null, "default")) {
                List<Object> expected = null;
                for (boolean retained : Arrays.asList(true, false)) {
                    List<Object> continued = new ArrayList<>();
                    Function<ReactorQLRecord, Publisher<?>> mapper = mapper(arguments, fallback, retained, Flux::collectList);
                    List<Object> actual = new ArrayList<>();
                    for (String row : Arrays.asList("ok", "bad", "missing")) {
                        actual.add(Flux.from(mapper.apply(record(row))).onErrorContinue((error, value) -> {
                            Assertions.assertSame(failure, error);
                            Assertions.assertSame(first, value);
                            continued.add(value);
                        }).single().block());
                    }
                    Assertions.assertEquals(Arrays.asList(first), continued);
                    if (retained) {
                        expected = actual;
                    } else {
                        Assertions.assertEquals(expected, actual, "count=" + count + ", default=" + fallback);
                    }
                }
            }
        }
    }

    @Test
    void directParameterDemandKeepsEvaluationOrderAndTiming() {
        List<String> expected = null;
        for (boolean retained : Arrays.asList(true, false)) {
            List<String> events = new ArrayList<>();
            ScalarValueMapper first = record -> { events.add("first"); return "first"; };
            ScalarValueMapper second = record -> { events.add("second"); return "second"; };
            Function<ReactorQLRecord, Publisher<?>> mapper = mapper(arguments(first, second, null), null,
                                                                    retained, source -> source);
            Publisher<?> result = mapper.apply(record("ok"));
            Assertions.assertTrue(events.isEmpty(), "No evaluation before subscription");
            StepVerifier.create(Flux.<Object>from(result), 0)
                        .then(() -> events.add("subscribed"))
                        .thenRequest(1)
                        .expectNext("first")
                        .then(() -> events.add("first received"))
                        .thenRequest(1)
                        .expectNext("second")
                        .verifyComplete();
            if (retained) {
                expected = events;
            } else {
                Assertions.assertEquals(expected, events);
            }
        }
    }

    @Test
    void failedArgumentRecoveryConsumerKeepsNativeTerminalSignals() {
        List<Object> expected = null;
        for (boolean retained : Arrays.asList(true, false)) {
            RuntimeException failure = new IllegalArgumentException("argument failure");
            IllegalStateException consumerFailure = new IllegalStateException("consumer failure");
            ScalarValueMapper first = record -> { throw failure; };
            List<Object> errors = new ArrayList<>();
            Function<ReactorQLRecord, Publisher<?>> mapper = mapper(
                    arguments(first, ScalarValueMapper.constant("tail"), null), null, retained, Flux::collectList);
            List<Object> signals = Flux.from(mapper.apply(record("bad")))
                    .onErrorContinue((error, value) -> {
                        errors.add(Arrays.asList(error.getClass().getName(), error.getMessage(),
                                                 value == first ? "first mapper" : value));
                        throw consumerFailure;
                    }).materialize().<Object>map(signal -> signal.isOnError()
                            ? Arrays.asList("error", signal.getThrowable().getClass().getName(),
                                            signal.getThrowable().getMessage())
                            : signal.isOnNext() ? Arrays.asList("value", signal.get())
                            : Arrays.asList("complete"))
                    .collectList().block();
            Assertions.assertFalse(errors.isEmpty(), "Fixture must reach parameter recovery");
            Assertions.assertTrue(signals.size() == 1 && "error".equals(((List<?>) signals.get(0)).get(0)));
            List<Object> actual = Arrays.asList(errors, signals);
            if (retained) {
                expected = actual;
            } else {
                Assertions.assertEquals(expected, actual);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<Function<ReactorQLRecord, Publisher<Object>>> arguments(
            ScalarValueMapper first, ScalarValueMapper second, ScalarValueMapper third) {
        return (List) (third == null ? Arrays.asList(first, second) : Arrays.asList(first, second, third));
    }

    private static Function<ReactorQLRecord, Publisher<?>> mapper(
            List<Function<ReactorQLRecord, Publisher<Object>>> arguments, Object fallback,
            boolean retained, Function<Flux<Object>, Publisher<?>> calculator) {
        FunctionMapFeature feature = new FunctionMapFeature("parameter_flow", arguments.size(), arguments.size(), calculator) {
            @Override
            protected List<Function<ReactorQLRecord, Publisher<Object>>> createParamMappers(
                    ReactorQLMetadata metadata, List<Expression> expressions) {
                // Both modes use exactly the same marker objects, including continuation data.
                return arguments;
            }

            @Override
            protected Publisher<Object> apply(ReactorQLRecord record,
                                              List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
                if (!retained) {
                    return super.apply(record, mappers);
                }
                return mapper.apply(Flux.fromIterable(mappers).concatMap(parameter -> {
                    Publisher<Object> values = parameter.apply(record);
                    return fallback == null ? values : Mono.fromDirect(values).defaultIfEmpty(fallback);
                }));
            }
        }.defaultValue(fallback);
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(arguments.size() == 2
                ? "select parameter_flow(a,b) v from test" : "select parameter_flow(a,b,c) v from test");
        return feature.createMapper(((SelectExpressionItem) metadata.getSql().getSelectItems().get(0)).getExpression(), metadata);
    }

    private static ReactorQLRecord record(String row) {
        return ReactorQLRecord.newRecord("test", row, new DefaultReactorQLContext(ignore -> Flux.empty()));
    }
}
