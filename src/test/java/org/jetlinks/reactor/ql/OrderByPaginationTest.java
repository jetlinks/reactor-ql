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

import net.sf.jsqlparser.statement.select.Distinct;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.DistinctFeature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderByPaginationTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "select this v from test order by this limit 2 offset 1",
            "select this v from test order by this limit 1,2",
            "select this v from test order by this offset 1 rows fetch next 2 rows only"
    })
    void shouldRetainOffsetPlusCountForEveryDialect(String sql) {
        ReactorQL query = boundedQuery(sql, 3);
        StepVerifier.create(query.start(Flux.range(0, 100).map(value -> 99 - value)))
                .expectNext(row(1), row(2))
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select top (2) this v from test order by this desc",
            "select this v from test order by this desc fetch first 2 rows only"
    })
    void shouldUseTheSameBoundedTopNForTopAndFetch(String sql) {
        StepVerifier.create(boundedQuery(sql, 2).start(Flux.range(0, 100)))
                .expectNext(row(99), row(98))
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select top (2) this v from test order by this",
            "select this v from test order by this fetch first 2 rows only"
    })
    void shouldStillObserveLateUpstreamErrors(String sql) {
        IllegalStateException failure = new IllegalStateException("late source error");
        StepVerifier.create(boundedQuery(sql, 2).start(Flux.range(0, 100).concatWith(Flux.error(failure))))
                .expectErrorSatisfies(error -> assertTrue(error == failure))
                .verify();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select this v from test order by this limit :count offset :start",
            "select this v from test order by this limit :start,:count",
            "select this v from test order by this offset :start rows fetch next :count rows only"
    })
    void shouldResolveBothPagingValuesForEachSubscription(String sql) {
        ReactorQL query = boundedQuery(sql, 5);
        ReactorQLContext context = ReactorQLContext.ofDatasource(ignored -> Flux.range(0, 100).map(value -> 99 - value));
        Flux<Map<String, Object>> result = query.start(context).map(ReactorQLRecord::asMap);
        context.bind("start", 1).bind("count", 2);
        StepVerifier.create(result).expectNext(row(1), row(2)).verifyComplete();
        context.bind("start", 3).bind("count", 1);
        StepVerifier.create(result).expectNext(row(3)).verifyComplete();
        context.bind("start", 0).bind("count", 2);
        StepVerifier.create(result).expectNext(row(0), row(1)).verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select this v from test order by this limit 3 offset 1",
            "select this v from test order by this offset 1 rows fetch next 3 rows only"
    })
    void shouldApplyTheBufferBudgetToOffsetPlusCount(String sql) {
        StepVerifier.create(boundedQuery(sql, 3).start(Flux.range(0, 100)))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof ReactorQLException);
                    assertTrue(error.getMessage().contains(DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS));
                })
                .verify();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select distinct this v from test order by this limit 2",
            "select distinctrow this v from test order by this limit 2"
    })
    void shouldNotTruncateBeforeDeduplication(String sql) {
        StepVerifier.create(boundedQuery(sql, 8).start(Flux.just(2, 1, 1, 3, 2)))
                .expectNext(row(1), row(2))
                .verifyComplete();
    }

    @Test
    void shouldApplyOffsetToDistinctRows() {
        StepVerifier.create(boundedQuery("select distinct this v from test order by this limit 2 offset 1", 8)
                                    .start(Flux.just(2, 1, 1, 3, 2)))
                .expectNext(row(2), row(3))
                .verifyComplete();
    }

    @Test
    void shouldKeepSortedFirstRowForDistinctOn() {
        ReactorQL query = boundedQuery("select distinct on (this.kind) this.rank v from test "
                                               + "order by this.rank desc limit 2", 8);
        StepVerifier.create(query.start(Flux.just(ranked("b", 7), ranked("a", 9), ranked("c", 6),
                                                   ranked("a", 10), ranked("b", 8))))
                .expectNext(row(10), row(8))
                .verifyComplete();
    }

    @Test
    void shouldKeepCustomDeduplicationAfterSortingAndBeforePagination() {
        DistinctFeature even = new DistinctFeature() {
            @Override
            public Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinctMapper(
                    Distinct distinct, ReactorQLMetadata metadata) {
                return source -> source.filter(record -> ((Number) record.getRecord()).intValue() % 2 == 0);
            }

            @Override
            public String getId() {
                return FeatureId.Distinct.defaultId.getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                  .sql("select distinct this v from test order by this limit 2")
                                  .feature(even)
                                  .build();
        StepVerifier.create(query.start(Flux.just(4, 3, 2, 1)))
                .expectNext(row(2), row(4))
                .verifyComplete();
    }

    @Test
    void shouldKeepFullSortBudgetWhenDeduplicationPreventsTopN() {
        StepVerifier.create(boundedQuery("select distinct this v from test order by this limit 1", 2)
                                    .start(Flux.just(1, 1, 2)))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof ReactorQLException);
                    assertTrue(error.getMessage().contains(DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS));
                })
                .verify();
    }

    @Test
    void shouldDescribeTheActualOrderOfSortingAndDeduplication() {
        String plan = ((DefaultReactorQL) boundedQuery("select distinct this v from test order by this limit 2", 8))
                .describeExecutionPlan();
        assertTrue(plan.indexOf("STATEFUL[order]") < plan.indexOf("STATEFUL[distinct]"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select top (0) this v from test order by this",
            "select this v from test order by this fetch first 0 rows only",
            "select distinct this v from test order by this limit 0"
    })
    void shouldNotSubscribeUpstreamForZeroCount(String sql) {
        AtomicInteger subscriptions = new AtomicInteger();
        StepVerifier.create(boundedQuery(sql, 1).start(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.never();
        }))).verifyComplete();
        assertEquals(0, subscriptions.get());
    }

    private static ReactorQL boundedQuery(String sql, int maxRows) {
        return ReactorQL.builder().sql(sql).setting(DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS, maxRows).build();
    }

    private static Map<String, Object> row(int value) {
        return Collections.singletonMap("v", value);
    }

    private static Map<String, Object> ranked(String kind, int rank) {
        Map<String, Object> values = new HashMap<>();
        values.put("kind", kind);
        values.put("rank", rank);
        return values;
    }
}
