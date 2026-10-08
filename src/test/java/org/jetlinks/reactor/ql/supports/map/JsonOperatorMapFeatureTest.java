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
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.DefaultReactorQLContext;
import org.jetlinks.reactor.ql.DefaultReactorQLRecord;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.TestRows;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class JsonOperatorMapFeatureTest {

    @Test
    void scalarDocumentKeepsNativePublisherConstructionTiming() {
        List<String> calls = new ArrayList<>();
        ValueMapFeature document = scalarFeature("scalar_doc", record -> {
            calls.add("document");
            return "{\"key\":\"value\"}";
        });
        Function<ReactorQLRecord, Publisher<?>> mapper = mapper("scalar_doc", document);
        Publisher<?> result = mapper.apply(record(1));
        // ScalarValueMapper.apply computes its document while constructing the native Publisher.
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("document")), calls);

        StepVerifier.create(Flux.from(result))
                    .assertNext(value -> Assertions.assertEquals("value", value))
                    .verifyComplete();
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("document")), calls);
    }

    @Test
    void jsonColumnKeepsMixedProjectionOrder() {
        List<String> calls = new ArrayList<>();
        Map<String, Object> row = new HashMap<String, Object>() {
            @Override
            public Object get(Object key) {
                if ("payload".equals(key) || "trace".equals(key)) {
                    calls.add(String.valueOf(key));
                }
                return super.get(key);
            }
        };
        row.put("payload", "{\"key\":\"value\"}");
        row.put("trace", "seen");
        ReactorQL query = ReactorQL.builder()
                                   .sql("select payload->>'key' json_value, trace traced from test")
                                   .build();
        Flux<Map<String, Object>> result = query.start(Flux.just(row));
        Assertions.assertTrue(calls.isEmpty());

        StepVerifier.create(result)
                    .expectNext(TestRows.row("json_value", "value", "traced", "seen"))
                    .verifyComplete();
        Assertions.assertTrue(calls.indexOf("trace") < calls.indexOf("payload"), calls.toString());
    }

    @Test
    void scalarDocumentKeepsEmptyAndErrorSignals() {
        IllegalStateException failure = new IllegalStateException("document failed");
        ValueMapFeature document = scalarFeature("scalar_doc", record -> {
            String mode = (String) record.getRecord();
            if ("empty".equals(mode)) {
                return null;
            }
            if ("error".equals(mode)) {
                throw failure;
            }
            return "{\"other\":1}";
        });
        Function<ReactorQLRecord, Publisher<?>> mapper = mapper("scalar_doc", document);

        StepVerifier.create(Flux.from(mapper.apply(record("empty"))))
                    .verifyComplete();
        StepVerifier.create(Flux.from(mapper.apply(record("missing"))))
                    .verifyComplete();
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> mapper.apply(record("error"))));
    }

    @Test
    void asyncDocumentKeepsContextAndCancellation() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature document = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.deferContextual(context -> {
                    Assertions.assertEquals("visible", context.get("marker"));
                    subscriptions.incrementAndGet();
                    return Mono.never().doOnCancel(() -> cancelled.set(true));
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("async_doc").getId();
            }
        };
        Function<ReactorQLRecord, Publisher<?>> mapper = mapper("async_doc", document);

        StepVerifier.create(Flux.from(mapper.apply(record(1)))
                                 .contextWrite(context -> context.put("marker", "visible")), 1)
                    .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void customMetadataKeepsJsonExpressionWrapper() {
        AtomicInteger subscriptions = new AtomicInteger();
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                "select payload->>'key' json_value from test") {
            @Override
            @SuppressWarnings("unchecked")
            public <T extends Publisher<? extends R>, R> Function<T, T> createWrapper(Object expression) {
                if (!(expression instanceof JsonExpression)) {
                    return Function.identity();
                }
                return source -> (T) Mono.deferContextual(context -> {
                    Assertions.assertEquals("visible", context.get("marker"));
                    subscriptions.incrementAndGet();
                    return Mono.from(source);
                });
            }
        };
        Assertions.assertFalse(metadata.supportsScalarFastPath());

        StepVerifier.create(new DefaultReactorQL(metadata)
                                    .start(Flux.just(Collections.singletonMap("payload", "{\"key\":\"value\"}")))
                                    .contextWrite(context -> context.put("marker", "visible")))
                    .expectNext(Collections.singletonMap("json_value", "value"))
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());
    }

    @Test
    void checkpointKeepsPublisherFallback() {
        ReactorQL query = ReactorQL.builder()
                                   .setting("checkpoint", true)
                                   .sql("select payload->>'key' json_value from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just(Collections.singletonMap("payload", "{\"key\":\"value\"}"))))
                    .expectNext(Collections.singletonMap("json_value", "value"))
                    .verifyComplete();
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

    private static Function<ReactorQLRecord, Publisher<?>> mapper(String name, ValueMapFeature feature) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select 1 from test");
        metadata.addFeature(feature);
        net.sf.jsqlparser.expression.Function document = new net.sf.jsqlparser.expression.Function();
        document.setName(name);
        document.setParameters(new ExpressionList(new Column("this")));
        JsonExpression expression = new JsonExpression();
        expression.setExpression(document);
        expression.addIdent("'key'", "->>");
        return JsonOperatorMapFeature.createMapper(expression, metadata);
    }

    private static ReactorQLRecord record(Object value) {
        return new DefaultReactorQLRecord("test", value, new DefaultReactorQLContext(ignored -> Flux.empty()));
    }
}
