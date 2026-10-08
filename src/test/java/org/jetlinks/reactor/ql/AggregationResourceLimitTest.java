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
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class AggregationResourceLimitTest {

    private static final int ABOVE_PREVIOUS_DEFAULT = 65_537;

    @Test
    void shouldPreserveUnboundedGroupAndCollectionDefaults() {
        Flux<Map<String, Object>> source = Flux
                .range(0, ABOVE_PREVIOUS_DEFAULT)
                .map(value -> Collections.<String, Object>singletonMap("val", value));

        ReactorQL grouped = ReactorQL
                .builder()
                .sql("select val,count(1) total from test group by val")
                .build();
        grouped.start(source)
               .count()
               .as(StepVerifier::create)
               .expectNext((long) ABOVE_PREVIOUS_DEFAULT)
               .verifyComplete();
        Assertions.assertTrue(((DefaultReactorQL) grouped)
                                      .describeExecutionPlan()
                                      .contains("maxActiveKeys=unbounded"));

        ReactorQL collected = ReactorQL
                .builder()
                .sql("select collect_list(val) values from test")
                .build();
        collected.start(source)
                 .as(StepVerifier::create)
                 .assertNext(result -> Assertions.assertEquals(
                         ABOVE_PREVIOUS_DEFAULT,
                         CastUtils.castArray(result.get("values")).size()
                 ))
                 .verifyComplete();
    }

    @Test
    void shouldBoundCompatibilityGroupKeysAndNotDoubleCountDuplicates() {
        ReactorQL withinLimit = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 4)
                .sql("select name,count(1) total from test group by name")
                .build();

        withinLimit.start(Flux.just(namedRow("a", 1), namedRow("a", 2)))
                   .as(StepVerifier::create)
                   .expectNext(map("name", "a", "total", 2L))
                   .verifyComplete();

        ReactorQL overflow = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 2)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 8)
                .sql("select name,count(1) total from test group by name")
                .build();

        overflow.start(Flux.just(namedRow("a", 1), namedRow("b", 2), namedRow("c", 3)))
                .as(StepVerifier::create)
                .expectErrorMatches(error -> isResourceLimit(
                        error,
                        DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS
                ))
                .verify();
    }

    @Test
    void shouldBoundCompatibilityBufferedRowsWithoutStalling() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_GROUP_CONCURRENCY, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 16)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 2)
                .sql("select name,count(1) total from test group by name")
                .build();

        query.start(Flux.just(namedRow("a", 1),
                              namedRow("b", 2),
                              namedRow("c", 3),
                              namedRow("d", 4))
                        .hide())
             .as(StepVerifier::create)
             .expectErrorMatches(error -> isResourceLimit(
                     error,
                     DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS
             ))
             .verify();
    }

    @Test
    void shouldReleaseCompatibilityBudgetAtWindowBoundary() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_GROUP_CONCURRENCY, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 1)
                .sql("select name,count(1) total from test group by _window(1),name")
                .build();

        query.start(Flux.just(namedRow("a", 1), namedRow("b", 2), namedRow("c", 3)))
             .as(StepVerifier::create)
             .expectNext(map("name", "a", "total", 1L))
             .expectNext(map("name", "b", "total", 1L))
             .expectNext(map("name", "c", "total", 1L))
             .verifyComplete();
    }

    @Test
    void shouldHonorDemandAndCancellationOnCheckpointCompatibilityPath() {
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL
                .builder()
                .setting("checkpoint", true)
                .setting(DefaultReactorQL.SETTING_GROUP_CONCURRENCY, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 2)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 2)
                .sql("select name,count(1) total from test group by _window(2),name")
                .build();
        Flux<Map<String, Object>> source = Flux
                .concat(Flux.just(namedRow("a", 1), namedRow("b", 2)), Flux.never())
                .hide()
                .doOnCancel(() -> cancelled.set(true));

        StepVerifier.create(query.start(source), 0)
                    .thenRequest(1)
                    .expectNextCount(1)
                    .thenCancel()
                    .verify();

        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldConsumeAsyncCompatibilitySourceWithoutStalling() {
        ReactorQL query = ReactorQL
                .builder()
                .setting("checkpoint", true)
                .setting(DefaultReactorQL.SETTING_GROUP_CONCURRENCY, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 2)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 2)
                .sql("select name,count(1) total from test group by _window(2),name")
                .build();

        query.start(Flux.just(namedRow("a", 1), namedRow("b", 2))
                        .hide()
                        .publishOn(Schedulers.parallel()))
             .collectList()
             .as(StepVerifier::create)
             .assertNext(results -> Assertions.assertEquals(2, results.size()))
             .verifyComplete();
    }

    @Test
    void shouldIsolateCompatibilityBudgetPerSubscription() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 1)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 2)
                .sql("select name,count(1) total from test group by name")
                .build();

        Flux<Map<String, Object>> first = query.start(Flux.just(namedRow("a", 1)));
        Flux<Map<String, Object>> second = query.start(Flux.just(namedRow("b", 2)));
        StepVerifier.create(Flux.zip(first, second))
                    .expectNextCount(1)
                    .verifyComplete();
    }

    @Test
    void shouldValidateAndDescribeCompatibilityGroupLimits() {
        for (Object value : Arrays.asList(
                0,
                DefaultReactorQL.HARD_MAX_GROUP_BUFFERED_ROWS + 1,
                "bad")) {
            ReactorQLException error = Assertions.assertThrows(
                    ReactorQLException.class,
                    () -> ReactorQL
                            .builder()
                            .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, value)
                            .sql("select name,count(1) total from test group by name")
                            .build()
            );
            Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, error.getI18nCode());
            Assertions.assertTrue(error.getMessage().contains(
                    DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS
            ));
        }

        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 12)
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS, 34)
                .sql("select name,count(1) total from test group by name")
                .build();
        String plan = ((DefaultReactorQL) query).describeExecutionPlan();
        Assertions.assertTrue(plan.contains("maxActiveKeys=12"));
        Assertions.assertTrue(plan.contains("maxBufferedRows=34"));
    }

    @Test
    void shouldBoundCollectionAggregates() {
        assertResourceLimit("select collect_list(val) values from test", rows(1, 2, 3));
        assertResourceLimit("select count(distinct val) total from test", rows(1, 2, 3));
        assertResourceLimit("select count(unique val) total from test", rows(1, 2, 3));
        assertResourceLimit("select distinct_count(val) total from test", rows(1, 2, 3));
        assertResourceLimit("select sum(distinct val) total from test", rows(1, 2, 3));
    }

    @Test
    void shouldCountRetainedStateRatherThanInputRows() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql("select count(distinct val) total from test")
                .build();

        query.start(rows(1, 1, 2, 2, 1))
             .as(StepVerifier::create)
             .expectNext(Collections.singletonMap("total", 2L))
             .verifyComplete();
    }

    @Test
    void shouldBoundExactCountStatePerGroup() {
        Flux<Map<String, Object>> source = Flux.just(
                namedRow("a", 1), namedRow("a", 1), namedRow("a", 2),
                namedRow("b", 1), namedRow("b", 2));
        for (String modifier : Arrays.asList("distinct", "unique")) {
            String sql = "select name,count(" + modifier + " val) total from test group by name";
            ReactorQL query = ReactorQL.builder()
                                       .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                                       .sql(sql)
                                       .build();
            Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                    .contains("STATEFUL[group,"));
            query.start(source)
                 .as(StepVerifier::create)
                 .expectNext(map("name", "a", "total", "distinct".equals(modifier) ? 2L : 1L))
                 .expectNext(map("name", "b", "total", 2L))
                 .verifyComplete();

            query.start(Flux.concat(source, Flux.just(namedRow("a", 3))))
                 .as(StepVerifier::create)
                 .expectErrorMatches(error -> isResourceLimit(
                         error, DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE))
                 .verify();
        }
    }

    @Test
    void shouldKeepUniqueResultsAfterManyRepeatedValues() {
        Flux<Map<String, Object>> source = Flux.concat(
                rows(1, 2, 1, 3, 4, 2, 5),
                Flux.range(0, 256).map(ignore -> Collections.<String, Object>singletonMap("val", 1))
        );
        ReactorQL count = ReactorQL.builder()
                                   .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 5)
                                   .sql("select count(unique val) total from test")
                                   .build();
        Flux<Map<String, Object>> counted = count.start(source);
        for (int i = 0; i < 2; i++) {
            counted.as(StepVerifier::create)
                   .expectNext(Collections.singletonMap("total", 3L))
                   .verifyComplete();
        }

        ReactorQL.builder()
                 .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 5)
                 .sql("select collect_list(unique val) values from test")
                 .build()
                 .start(source)
                 .as(StepVerifier::create)
                 .assertNext(result -> Assertions.assertEquals(Arrays.asList(
                         Collections.singletonMap("val", 3),
                         Collections.singletonMap("val", 4),
                         Collections.singletonMap("val", 5)), CastUtils.castArray(result.get("values"))))
                 .verifyComplete();

        ReactorQL.builder()
                 .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 5)
                 .sql("select avg(unique val) total from test")
                 .build()
                 .start(source)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 4.0D))
                 .verifyComplete();

        count.start(Flux.empty())
             .as(StepVerifier::create)
             .expectNext(Collections.singletonMap("total", 0L))
             .verifyComplete();
        assertResourceLimit("select count(unique val) total from test", rows(1, 1, 2, 2, 3));
    }

    @Test
    void shouldKeepUniqueSourceErrorAndCancellation() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select count(unique val) total from test")
                                   .build();
        IllegalStateException failure = new IllegalStateException("source failure");
        query.start(Flux.concat(rows(1, 1), Flux.error(failure)))
             .as(StepVerifier::create)
             .expectErrorSatisfies(error -> Assertions.assertSame(failure, error))
             .verify();

        AtomicBoolean cancelled = new AtomicBoolean();
        StepVerifier.create(query.start(Flux.concat(rows(1, 1), Flux.never())
                                       .doOnCancel(() -> cancelled.set(true))), 0)
                    .thenRequest(1)
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldKeepBoundedDistinctValuesStreaming() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 3)
                .sql("select take(distinct val, 3) value from test")
                .build();
        AtomicBoolean cancelled = new AtomicBoolean();
        Flux<Map<String, Object>> unbounded = Flux
                .concat(rows(3, 1, 3, 2, 4), Flux.never())
                .doOnCancel(() -> cancelled.set(true));

        query.start(unbounded)
             .as(StepVerifier::create)
             .expectNext(Collections.singletonMap("value", 3))
             .expectNext(Collections.singletonMap("value", 1))
             .expectNext(Collections.singletonMap("value", 2))
             .verifyComplete();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldReleaseCollectionStateAtWindowBoundary() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql("select collect_list(val) values from test group by _window(2)")
                .build();

        query.start(rows(1, 2, 3, 4))
             .as(StepVerifier::create)
             .assertNext(result -> Assertions.assertEquals(
                     2,
                     CastUtils.castArray(result.get("values")).size()
             ))
             .assertNext(result -> Assertions.assertEquals(
                     2,
                     CastUtils.castArray(result.get("values")).size()
             ))
             .verifyComplete();
    }

    @Test
    void shouldBoundCollectRowAndOverwriteDuplicateKeys() {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql("select collect_row(name,val) rows from test")
                .build();

        query.start(Flux.just(
                     namedRow("a", 1),
                     namedRow("a", 2),
                     namedRow("b", 3)
             ))
             .as(StepVerifier::create)
             .assertNext(result -> Assertions.assertEquals(
                     map("a", 2, "b", 3),
                     result.get("rows")
             ))
             .verifyComplete();

        query.start(Flux.just(
                     namedRow("a", 1),
                     namedRow("b", 2),
                     namedRow("c", 3)
             ))
             .as(StepVerifier::create)
             .expectErrorMatches(error -> error instanceof ReactorQLException
                     && ReactorQLException.RESOURCE_LIMIT.equals(
                             ((ReactorQLException) error).getI18nCode())
                     && error.getMessage().contains(
                             DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE))
             .verify();
    }

    @Test
    void shouldCollectRowAndReleaseItsStatePerNativeWindow() {
        String sql = "select collect_row(name,val) rows from test group by _window(2)";
        ReactorQL fused = ReactorQL.builder().sql(sql).build();
        ReactorQL fallback = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();

        Assertions.assertTrue(((DefaultReactorQL) fused)
                                      .describeExecutionPlan()
                                      .contains("STATEFUL[group,"));
        List<Map<String, Object>> expected = fallback
                .start(Flux.just(
                        namedRow("a", 1),
                        namedRow("b", 2),
                        namedRow("c", 3),
                        namedRow("d", 4)
                ))
                .collectList()
                .block();
        Assertions.assertNotNull(expected);

        fused.start(Flux.just(
                     namedRow("a", 1),
                     namedRow("b", 2),
                     namedRow("c", 3),
                     namedRow("d", 4)
             ))
             .collectList()
             .as(StepVerifier::create)
             .expectNext(expected)
             .verifyComplete();
    }

    @Test
    void shouldRejectInvalidCollectionLimit() {
        for (Object value : Arrays.asList(
                0,
                DefaultReactorQL.HARD_MAX_AGGREGATE_COLLECTION_SIZE + 1,
                "bad")) {
            ReactorQLException error = Assertions.assertThrows(
                    ReactorQLException.class,
                    () -> ReactorQL
                            .builder()
                            .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, value)
                            .sql("select collect_list(val) values from test")
                            .build()
            );
            Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, error.getI18nCode());
            Assertions.assertTrue(error.getMessage().contains(
                    DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE
            ));
        }
    }

    private static void assertResourceLimit(String sql, Flux<Map<String, Object>> source) {
        ReactorQL query = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql(sql)
                .build();

        query.start(source)
             .as(StepVerifier::create)
             .expectErrorMatches(error -> error instanceof ReactorQLException
                     && ReactorQLException.RESOURCE_LIMIT.equals(
                             ((ReactorQLException) error).getI18nCode())
                     && error.getMessage().contains(
                             DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE))
             .verify();
    }

    private static boolean isResourceLimit(Throwable error, String setting) {
        return error instanceof ReactorQLException
                && ReactorQLException.RESOURCE_LIMIT.equals(
                        ((ReactorQLException) error).getI18nCode())
                && error.getMessage().contains(setting);
    }

    private static Flux<Map<String, Object>> rows(int... values) {
        return Flux.range(0, values.length)
                   .map(index -> Collections.<String, Object>singletonMap("val", values[index]));
    }

    private static Map<String, Object> namedRow(String name, int value) {
        Map<String, Object> row = new HashMap<>();
        row.put("name", name);
        row.put("val", value);
        return row;
    }

    private static Map<String, Object> map(Object... entries) {
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            values.put(String.valueOf(entries[i]), entries[i + 1]);
        }
        return values;
    }
}
