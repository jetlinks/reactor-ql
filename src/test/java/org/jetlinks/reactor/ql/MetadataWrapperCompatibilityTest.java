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

import net.sf.jsqlparser.statement.select.PlainSelect;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class MetadataWrapperCompatibilityTest {

    private static final String WRAPPER_CONTEXT_KEY = "metadata-wrapper-test";
    private static final String WRAPPER_CONTEXT_VALUE = "available";

    @Test
    void shouldKeepCustomExpressionWrappersForScalarNumericFunctionJsonAndWhere() {
        WrappingMetadata metadata = new WrappingMetadata(
                "select score + 1 number, json_get(json, '$.value') json_value "
                        + "from test where score + 1 = 2"
        );

        StepVerifier.create(new DefaultReactorQL(metadata)
                                    .start(Flux.just(TestRows.row("score", 1, "json", "{\"value\":3}")))
                                    .contextWrite(context -> context.put(WRAPPER_CONTEXT_KEY, WRAPPER_CONTEXT_VALUE)))
                    .expectNext(TestRows.row("number", 2L, "json_value", 3))
                    .verifyComplete();

        Assertions.assertEquals(1, metadata.querySubscriptions.get());
        Assertions.assertTrue(metadata.expressionSubscriptions.get() >= 3);
        Assertions.assertTrue(metadata.wrappedExpressions.stream().anyMatch(expression -> expression.contains("score + 1")),
                              metadata.wrappedExpressions.toString());
        Assertions.assertTrue(metadata.wrappedExpressions.stream().anyMatch(expression -> expression.contains("json_get(json")),
                              metadata.wrappedExpressions.toString());
    }

    @Test
    void shouldKeepFilterFallbackWrapperAndForwardErrorWithContext() {
        WrappingMetadata filterMetadata = new WrappingMetadata("select * from test where score || ''");
        ReactorQLRecord record = new DefaultReactorQLRecord(
                "test",
                Collections.singletonMap("score", "ok"),
                new DefaultReactorQLContext(ignore -> Flux.empty())
        );

        StepVerifier.create(FilterFeature.createPredicateNow(filterMetadata.getSql().getWhere(), filterMetadata)
                                        .apply(record, "ok")
                                        .contextWrite(context -> context.put(WRAPPER_CONTEXT_KEY, WRAPPER_CONTEXT_VALUE)))
                    .expectNext(true)
                    .verifyComplete();
        Assertions.assertTrue(filterMetadata.expressionSubscriptions.get() >= 2);

        WrappingMetadata errorMetadata = new WrappingMetadata("select score / 0 value from test");
        StepVerifier.create(new DefaultReactorQL(errorMetadata)
                                    .start(Flux.just(Collections.singletonMap("score", 1)))
                                    .contextWrite(context -> context.put(WRAPPER_CONTEXT_KEY, WRAPPER_CONTEXT_VALUE)))
                    .expectError()
                    .verify();
        Assertions.assertEquals(1, errorMetadata.querySubscriptions.get());
        Assertions.assertTrue(errorMetadata.expressionSubscriptions.get() > 0);
    }

    @Test
    void shouldRequireExplicitScalarFastPathOptInForDefaultMetadataSubclasses() {
        Assertions.assertTrue(new DefaultReactorQLMetadata("select 1 from dual").supportsScalarFastPath());
        Assertions.assertFalse(new WrappingMetadata("select 1 from dual").supportsScalarFastPath());
    }

    private static final class WrappingMetadata extends DefaultReactorQLMetadata {

        private final AtomicInteger querySubscriptions = new AtomicInteger();
        private final AtomicInteger expressionSubscriptions = new AtomicInteger();
        private final List<String> wrappedExpressions = new CopyOnWriteArrayList<>();

        private WrappingMetadata(String sql) {
            super(sql);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Publisher<? extends R>, R> Function<T, T> createWrapper(Object expression) {
            boolean query = expression instanceof PlainSelect;
            String name = String.valueOf(expression);
            return source -> source instanceof Mono
                    ? (T) Mono.deferContextual(context -> {
                        recordWrapperSubscription(query, name, context.hasKey(WRAPPER_CONTEXT_KEY));
                        return Mono.from(source);
                    })
                    : (T) Flux.deferContextual(context -> {
                        recordWrapperSubscription(query, name, context.hasKey(WRAPPER_CONTEXT_KEY));
                        return Flux.from(source);
                    });
        }

        private void recordWrapperSubscription(boolean query, String expression, boolean hasContext) {
            if (!hasContext) {
                throw new AssertionError("wrapper lost Reactor Context");
            }
            if (query) {
                querySubscriptions.incrementAndGet();
            } else {
                expressionSubscriptions.incrementAndGet();
                wrappedExpressions.add(expression);
            }
        }
    }
}
