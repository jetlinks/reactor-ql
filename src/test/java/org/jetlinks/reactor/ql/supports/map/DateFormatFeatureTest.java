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
import org.jetlinks.reactor.ql.TestRows;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class DateFormatFeatureTest {

    @Test
    void scalarInputStaysColdAndKeepsMixedProjectionOrder() {
        List<String> calls = new ArrayList<>();
        ValueMapFeature date = scalarFeature("scalar_date", record -> {
            calls.add("date");
            return 0L;
        });
        ValueMapFeature trace = scalarFeature("scalar_trace", record -> {
            calls.add("trace");
            return "seen";
        });
        ReactorQL query = ReactorQL.builder()
                                   .feature(date, trace)
                                   .sql("select date_format(scalar_date(this), 'yyyy-MM-dd', 'UTC') formatted,"
                                                + "scalar_trace(this) traced from test")
                                   .build();
        Flux<Map<String, Object>> result = query.start(Flux.just(1));
        Assertions.assertTrue(calls.isEmpty());

        StepVerifier.create(result)
                    .expectNext(TestRows.row("formatted", "1970-01-01", "traced", "seen"))
                    .verifyComplete();
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("trace", "date")), calls);
    }

    @Test
    void scalarInputKeepsEmptyAndErrorSignals() {
        IllegalStateException failure = new IllegalStateException("date failed");
        ValueMapFeature date = scalarFeature("scalar_date", record -> {
            String mode = (String) record.getRecord();
            if ("empty".equals(mode)) {
                return null;
            }
            if ("error".equals(mode)) {
                throw failure;
            }
            return 0L;
        });
        ReactorQL query = ReactorQL.builder()
                                   .feature(date)
                                   .sql("select date_format(scalar_date(this), 'yyyy-MM-dd', 'UTC') formatted from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just("empty")))
                    .expectNext(Collections.emptyMap())
                    .verifyComplete();
        StepVerifier.create(query.start(Flux.just("error")))
                    .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
                    .verify();
    }

    @Test
    void asyncInputKeepsContextAndCancellation() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        ValueMapFeature date = new ValueMapFeature() {
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
                return FeatureId.ValueMap.of("async_date").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                   .feature(date)
                                   .sql("select date_format(async_date(this), 'yyyy-MM-dd', 'UTC') formatted from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just(1))
                                 .contextWrite(context -> context.put("marker", "visible")), 1)
                    .then(() -> Assertions.assertEquals(1, subscriptions.get()))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
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
}
