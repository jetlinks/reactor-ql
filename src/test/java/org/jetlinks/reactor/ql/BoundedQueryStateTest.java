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

import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

class BoundedQueryStateTest {

    private static final int ABOVE_PREVIOUS_DEFAULT = 65_537;

    @Test
    void shouldPreserveUnboundedDistinctAndSetOperationDefaults() {
        ReactorQL distinct = ReactorQL
                .builder()
                .sql("select distinct val from test")
                .build();
        distinct.start(Flux
                              .range(0, ABOVE_PREVIOUS_DEFAULT)
                              .map(BoundedQueryStateTest::row))
                .count()
                .as(StepVerifier::create)
                .expectNext((long) ABOVE_PREVIOUS_DEFAULT)
                .verifyComplete();

        ReactorQL union = ReactorQL
                .builder()
                .sql("select t.v from (select v from t1 union select v from t2) t")
                .build();
        union.start(name -> "t1".equals(name)
                        ? Flux.range(0, ABOVE_PREVIOUS_DEFAULT)
                              .map(value -> Collections.singletonMap("v", value))
                        : Flux.empty())
             .count()
             .as(StepVerifier::create)
             .expectNext((long) ABOVE_PREVIOUS_DEFAULT)
             .verifyComplete();
    }

    @Test
    void shouldBoundDistinctByRetainedKeys() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS, 2)
                .sql("select distinct val from test")
                .build();

        query.start(rows(1, 1, 2, 2, 3))
             .as(StepVerifier::create)
             .expectNext(row(1), row(2))
             .expectErrorMatches(error -> resourceLimit(
                     error,
                     DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS
             ))
             .verify();
    }

    @Test
    void shouldKeepDistinctStatePerSubscriptionAndHonorCancellation() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS, 2)
                .sql("select distinct val from test")
                .build();

        query.start(rows(1, 1, 2))
             .as(StepVerifier::create)
             .expectNext(row(1), row(2))
             .verifyComplete();
        query.start(rows(3, 3, 4))
             .as(StepVerifier::create)
             .expectNext(row(3), row(4))
             .verifyComplete();

        AtomicBoolean cancelled = new AtomicBoolean();
        Flux<Map<String, Object>> unbounded = Flux
                .concat(rows(1), Flux.never())
                .doOnCancel(() -> cancelled.set(true));
        query.start(unbounded)
             .take(1)
             .as(StepVerifier::create)
             .expectNext(row(1))
             .verifyComplete();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldKeepUnionAllStreamingAndBoundUnion() {
        ReactorQL unionAll = setQuery(
                "union all",
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                1
        );
        unionAll.start(sources(new Integer[]{0, 1}, new Integer[]{1, 2}))
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();

        ReactorQL union = setQuery(
                "union",
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                2
        );
        union.start(sources(new Integer[]{0, 1}, new Integer[]{1, 2}))
             .as(StepVerifier::create)
             .expectNextCount(2)
             .expectErrorMatches(error -> resourceLimit(
                     error,
                     DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS
             ))
             .verify();
    }

    @Test
    void shouldBoundMaterializedSetAndCountOnlyDistinctOutputKeys() {
        ReactorQL intersect = setQuery(
                "intersect",
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                2
        );
        intersect.start(sources(new Integer[]{0, 1}, new Integer[]{0, 1, 2}))
                 .as(StepVerifier::create)
                 .expectErrorMatches(error -> resourceLimit(
                         error,
                         DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS
                 ))
                 .verify();
        intersect.start(sources(new Integer[]{0, 0, 1, 1}, new Integer[]{0, 1}))
                 .as(StepVerifier::create)
                 .expectNext(valueRow(0), valueRow(1))
                 .verifyComplete();

        ReactorQL minus = setQuery(
                "minus",
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                2
        );
        minus.start(sources(new Integer[]{0, 0, 1}, new Integer[]{9}))
             .as(StepVerifier::create)
             .expectNext(valueRow(0), valueRow(1))
             .verifyComplete();
    }

    private static ReactorQL setQuery(String operation, String setting, Object maxRows) {
        return ReactorQL
                .builder()
                .setting(setting, maxRows)
                .sql("select t.v from (",
                     "select v from t1 ", operation,
                     " select v from t2",
                     ") t")
                .build();
    }

    private static Function<String, Publisher<?>> sources(Integer[] left, Integer[] right) {
        return name -> values("t1".equals(name) ? left : right);
    }

    private static Flux<Map<String, Object>> values(Integer[] values) {
        return Flux
                .fromArray(values)
                .map(value -> Collections.<String, Object>singletonMap("v", value));
    }

    private static Flux<Map<String, Object>> rows(int... values) {
        return Flux.range(0, values.length).map(index -> row(values[index]));
    }

    private static Map<String, Object> row(int value) {
        return Collections.<String, Object>singletonMap("val", value);
    }

    private static Map<String, Object> valueRow(int value) {
        return Collections.<String, Object>singletonMap("t.v", value);
    }

    private static boolean resourceLimit(Throwable error, String setting) {
        return error instanceof ReactorQLException
                && ReactorQLException.RESOURCE_LIMIT.equals(
                        ((ReactorQLException) error).getI18nCode())
                && error.getMessage().contains(setting);
    }
}
