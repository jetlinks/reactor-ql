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
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

class CollectListIncrementalTest {

    private static final String SQL =
            "select type,collect_list(score,'label') values from test group by type";

    @Test
    void shouldFuseSynchronousColumnsAndKeepCollectListResultSemantics() {
        ReactorQL optimized = ReactorQL.builder().sql(SQL).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(SQL)
                .build();
        String plan = ((DefaultReactorQL) optimized).describeExecutionPlan();
        Assertions.assertTrue(plan.contains("ASYNC_OR_STATEFUL[projection]")
                                      && plan.contains("ASYNC_OR_STATEFUL[projection]"), plan);

        Flux<Map<String, Object>> source = Flux.just(
                row("a", 1, "first"),
                row("b", 2, "second"),
                row("a", 3, "third"));
        List<Map<String, Object>> expected = legacy.start(source).collectList().block();
        List<Map<String, Object>> actual = optimized.start(source).collectList().block();
        Assertions.assertEquals(expected, actual);
        Assertions.assertNotNull(actual);

        Object values = actual.get(0).get("values");
        Assertions.assertInstanceOf(ArrayList.class, values);
        List<Object> collected = (List<Object>) values;
        Assertions.assertInstanceOf(LinkedHashMap.class, collected.get(0));
        Map<String, Object> collectedRow = (Map<String, Object>) collected.get(0);
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("score", "label")), new ArrayList<>(collectedRow.keySet()));
        collectedRow.put("mutable", true);
        Assertions.assertEquals(true, collectedRow.get("mutable"));
        collected.add(new LinkedHashMap<>());
        Assertions.assertEquals(3, collected.size());
    }

    @Test
    void shouldUseTheExistingLimitAndReactiveTerminationSignals() {
        ReactorQL query = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql("select collect_list(score) values from test")
                .build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(query.start(Flux.just(row("a", 1, "one"),
                                               row("a", 2, "two"),
                                               row("a", 3, "three"))))
                .expectErrorMatches(error -> error instanceof ReactorQLException
                        && ReactorQLException.RESOURCE_LIMIT.equals(
                        ((ReactorQLException) error).getI18nCode()))
                .verify();

        RuntimeException failure = new RuntimeException("source failed");
        StepVerifier.create(query.start(Flux.concat(Flux.just(row("a", 1, "one")),
                                                   Flux.error(failure))))
                .expectErrorMatches(error -> error == failure)
                .verify();

        AtomicBoolean cancelled = new AtomicBoolean();
        StepVerifier.create(query.start(Flux.concat(Flux.just(row("a", 1, "one")), Flux.never())
                                             .doOnCancel(() -> cancelled.set(true))), 0)
                .thenRequest(1)
                .thenCancel()
                .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldMapTheOverLimitRowBeforeReportingTheExistingLimit() {
        List<Integer> mappedScores = new ArrayList<>();
        PropertyFeature property = new org.jetlinks.reactor.ql.supports.DefaultPropertyFeature() {
            @Override
            public Optional<Object> getProperty(Object name, Object source) {
                if ("score".equals(name) && source instanceof Map
                        && ((Map<?, ?>) source).get("score") instanceof Integer) {
                    mappedScores.add((Integer) ((Map<?, ?>) source).get("score"));
                }
                return super.getProperty(name, source);
            }
        };
        ReactorQL query = ReactorQL.builder()
                .feature(property)
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 1)
                .sql("select collect_list(score) values from test")
                .build();

        StepVerifier.create(query.start(Flux.just(row("a", 1, "one"), row("a", 2, "two"))))
                .expectErrorMatches(error -> error instanceof ReactorQLException
                        && ReactorQLException.RESOURCE_LIMIT.equals(
                        ((ReactorQLException) error).getI18nCode()))
                .verify();
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList(1, 2)), mappedScores);
    }

    @Test
    void shouldResetCollectionStateAtEachCountWindow() {
        String sql = "select type,collect_list(score) values from test group by _window(2),type";
        ReactorQL optimized = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql(sql)
                .build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .setting(DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE, 2)
                .sql(sql)
                .build();
        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        Flux<Map<String, Object>> source = Flux.just(
                row("a", 1, "one"), row("a", 2, "two"),
                row("a", 3, "three"), row("a", 4, "four"));
        List<Map<String, Object>> actual = optimized.start(source).collectList().block();
        Assertions.assertEquals(legacy.start(source).collectList().block(), actual);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(2, actual.size());
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList(1, 2)), scores(actual.get(0)));
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList(3, 4)), scores(actual.get(1)));
        Assertions.assertNotSame(actual.get(0).get("values"), actual.get(1).get("values"));
        Assertions.assertInstanceOf(ArrayList.class, actual.get(0).get("values"));
        Assertions.assertInstanceOf(ArrayList.class, actual.get(1).get("values"));
    }

    @Test
    void shouldPreserveGroupMetadataOnEachCollectedInputRow() {
        for (String sql : Collections.unmodifiableList(Arrays.asList(
                "select type,collect_list('_group_by_key') values from test group by type",
                "select type,collect_list('this') values from test group by type"))) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql)
                    .build();

            Flux<Map<String, Object>> source = Flux.just(
                    row("a", 1, "first"),
                    row("b", 2, "second"),
                    row("a", 3, "third"));
            List<Map<String, Object>> expected = legacy.start(source).collectList().block();
            List<Map<String, Object>> actual = optimized.start(source).collectList().block();

            Assertions.assertEquals(expected, actual, sql);
        }
    }

    @Test
    void shouldPreserveNativeGroupKeysWithoutMutatingUpstreamKeyList() {
        String sql = "select a,b,_group_by_key,collect_list(score) values from test group by a,b";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();

        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        List<Map<String, Object>> expected = legacy.start(table -> Flux.just(
                        rowWithGroupKey(new ArrayList<>(Collections.unmodifiableList(Arrays.asList("pre"))))))
                .collectList()
                .block();
        List<Object> upstreamKeys = new ArrayList<>(Collections.unmodifiableList(Arrays.asList("pre")));
        ReactorQLRecord input = rowWithGroupKey(upstreamKeys);
        List<Map<String, Object>> actual = optimized.start(table -> Flux.just(input))
                .collectList()
                .block();

        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("pre")), upstreamKeys);
        List<Object> resultKeys = (List<Object>) actual.get(0).get("_group_by_key");
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("pre", "A", "B")), resultKeys);
        List<Object> inputKeys = (List<Object>) input.getRecordValue(GroupFeature.groupByKeyContext);
        // Native projection preserves the key value already bound on the input Record;
        // GroupFeature still copies the caller's original upstream key list when appending keys.
        Assertions.assertSame(resultKeys, inputKeys);
        resultKeys.add("result-only");
        Assertions.assertTrue(inputKeys.contains("result-only"));
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("pre")), upstreamKeys);
    }

    @Test
    void shouldExposeEachResolvedGroupKeyToFollowingDimensions() {
        for (String sql : Collections.unmodifiableList(Arrays.asList(
                "select a,count(1) total from test group by a,_group_by_key",
                "select a,count(1) total from test group by a,_window(1),_group_by_key"))) {
            ReactorQL optimized = ReactorQL.builder().sql(sql).build();
            ReactorQL legacy = ReactorQL.builder()
                    .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                    .sql(sql)
                    .build();

            Flux<Map<String, Object>> source = Flux.just(groupingRow());
            List<Map<String, Object>> expected = legacy.start(source).collectList().block();
            List<Map<String, Object>> actual = optimized.start(source).collectList().block();

            Assertions.assertEquals(expected, actual, sql);
            Assertions.assertEquals(1, actual.size(), sql);
            Assertions.assertEquals(1L, actual.get(0).get("total"), sql);
        }
    }

    @Test
    void shouldKeepInputGroupKeysInCompactOutputRecords() {
        String sql = "select a,count(1) total from test group by a";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        Assertions.assertTrue(((DefaultReactorQL) optimized).describeExecutionPlan()
                .contains("ASYNC_OR_STATEFUL[projection]"));

        List<Object> upstreamKeys = new ArrayList<>(Collections.unmodifiableList(Arrays.asList("pre")));
        ReactorQLContext optimizedContext = ReactorQLContext.ofDatasource(
                ignore -> Flux.just(rowWithGroupKey(upstreamKeys)));
        ReactorQLContext legacyContext = ReactorQLContext.ofDatasource(
                ignore -> Flux.just(rowWithGroupKey(new ArrayList<>(Collections.unmodifiableList(Arrays.asList("pre"))))));

        ReactorQLRecord expected = legacy.start(legacyContext).single().block();
        ReactorQLRecord actual = optimized.start(optimizedContext).single().block();
        Assertions.assertEquals(expected.asMap(), actual.asMap());
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("pre", "A")), GroupFeature.getGroupKey(actual));
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("pre")), upstreamKeys);
        List<Object> outputKeys = (List<Object>) actual.getRecordValue(GroupFeature.groupByKeyContext);
        outputKeys.add("result-only");
        Assertions.assertEquals(Collections.unmodifiableList(Arrays.asList("pre")), upstreamKeys);
    }

    @Test
    void shouldKeepEmptyGlobalCollectListEquivalentToTheCompatiblePath() {
        String sql = "select collect_list(score) values from test";
        ReactorQL optimized = ReactorQL.builder().sql(sql).build();
        ReactorQL legacy = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(sql)
                .build();
        List<Map<String, Object>> actual = optimized.start(Flux.empty()).collectList().block();
        Assertions.assertEquals(legacy.start(Flux.empty()).collectList().block(), actual);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertInstanceOf(ArrayList.class, actual.get(0).get("values"));
        Assertions.assertTrue(((List<?>) actual.get(0).get("values")).isEmpty());
    }

    @Test
    void shouldKeepContextAndConservativeModesOnTheCompatiblePath() {
        ReactorQL query = ReactorQL.builder()
                .sql("select collect_list(score) values from test")
                .build();
        StepVerifier.create(query.start(Flux.deferContextual(context -> {
                                     Assertions.assertEquals("visible", context.get("marker"));
                                     return Flux.just(row("a", 1, "one"));
                                 }))
                                 .contextWrite(context -> context.put("marker", "visible")), 0)
                .thenRequest(1)
                .expectNextMatches(result -> result.containsKey("values"))
                .verifyComplete();

        for (String sql : Collections.unmodifiableList(Arrays.asList(
                "select collect_list(distinct score) values from test",
                "select collect_list(unique score) values from test",
                "select collect_list() values from test",
                "select collect_list((select score from test)) values from test"))) {
            ReactorQL fallback = ReactorQL.builder().sql(sql).build();
            Assertions.assertFalse(((DefaultReactorQL) fallback).describeExecutionPlan()
                    .contains("STATEFUL[fused"), sql);
        }
    }

    private static Map<String, Object> row(String type, int score, String label) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("type", type);
        row.put("score", score);
        row.put("label", label);
        return row;
    }

    private static Map<String, Object> groupingRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("a", "A");
        return row;
    }

    private static ReactorQLRecord rowWithGroupKey(List<Object> groupKeys) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("a", "A");
        row.put("b", "B");
        row.put("score", 1);
        return ReactorQLRecord
                .newRecord(null, row, new DefaultReactorQLContext(ignore -> Flux.empty()))
                .addRecord(GroupFeature.groupByKeyContext, groupKeys);
    }

    private static List<Integer> scores(Map<String, Object> result) {
        return ((List<Map<String, Integer>>) result.get("values"))
                .stream()
                .map(value -> value.get("score"))
                .collect(java.util.stream.Collectors.toList());
    }
}
