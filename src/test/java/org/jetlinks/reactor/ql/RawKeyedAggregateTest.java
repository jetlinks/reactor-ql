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

import net.sf.jsqlparser.statement.select.FromItem;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.supports.group.GroupByValueFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class RawKeyedAggregateTest {

    @Test
    void shouldMatchPublisherPathForBothCountWindowPositionsAndWhere() {
        List<Map<String, Object>> rows = Arrays.asList(
                row("a", 1), row("b", 2), row("a", 3), row("b", 4),
                row("a", 5), row("b", 6), row("a", 7), row("b", 8));
        String[] sqls = {
                "select key,count(1) total,sum(score) sum,avg(score) avg,max(score) max "
                        + "from test where score > 0 group by _window(4),key",
                "select key,count(1) total,sum(score) sum,avg(score) avg,max(score) max "
                        + "from test group by key,_window(2)"
        };

        for (String sql : sqls) {
            ReactorQL raw = ReactorQL.builder().sql(sql).build();
            ReactorQL publisher = ReactorQL.builder()
                                          .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                          .sql(sql)
                                          .build();
            Assertions.assertTrue(((DefaultReactorQL) raw).describeExecutionPlan()
                                                         .contains("ASYNC_OR_STATEFUL[projection]"), sql);
            List<Map<String, Object>> actual = raw.start(Flux.fromIterable(rows)).collectList().block();
            List<Map<String, Object>> expected = publisher.start(Flux.fromIterable(rows)).collectList().block();
            Assertions.assertEquals(multiset(expected), multiset(actual), sql);
        }
    }

    @Test
    void shouldPreserveMixedRecordInputAndIndependentOutputGroupKeys() {
        String sql = "select key,count(1) total,sum(score) sum from test group by _window(4),key";
        ReactorQL raw = ReactorQL.builder().sql(sql).build();
        ReactorQL publisher = ReactorQL.builder()
                                      .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                      .sql(sql)
                                      .build();
        List<Map<String, Object>> expected = publisher.start(mixedSource()).collectList().block();
        List<Map<String, Object>> actual = raw.start(mixedSource()).collectList().block();
        Assertions.assertEquals(multiset(expected), multiset(actual));

        List<ReactorQLRecord> records = raw.start(
                new DefaultReactorQLContext(ignore -> mixedSource()))
                                           .collectList()
                                           .block();
        Assertions.assertNotNull(records);
        Assertions.assertEquals(2, records.size());
        List<Object> firstKeys = GroupFeature.getGroupKey(records.get(0));
        List<Object> secondKeys = GroupFeature.getGroupKey(records.get(1));
        Assertions.assertEquals(1, firstKeys.size());
        Assertions.assertEquals(1, secondKeys.size());
        Assertions.assertEquals(records.get(0).asMap().get("key"), firstKeys.get(0));
        Assertions.assertEquals(records.get(1).asMap().get("key"), secondKeys.get(0));
        firstKeys.add("reader-only");
        Assertions.assertEquals(1, GroupFeature.getGroupKey(records.get(0)).size());
        Assertions.assertEquals(1, GroupFeature.getGroupKey(records.get(1)).size());
    }

    @Test
    void shouldKeepArrayValuedSingleDimensionAsOneKey() {
        Object[] key = {"device", 1};
        Map<String, Object> first = new HashMap<>();
        first.put("key", key);
        first.put("score", 1);
        Map<String, Object> second = new HashMap<>();
        second.put("key", key);
        second.put("score", 2);
        String sql = "select key,count(1) total from test group by _window(2),key";
        ReactorQL query = ReactorQL.builder().sql(sql).build();
        Assertions.assertTrue(((DefaultReactorQL) query).describeExecutionPlan().contains("ASYNC_OR_STATEFUL[projection]"));

        List<ReactorQLRecord> records = query.start(new DefaultReactorQLContext(
                ignore -> Flux.just(first, second))).collectList().block();
        Assertions.assertNotNull(records);
        Assertions.assertEquals(1, records.size());
        Assertions.assertSame(key, records.get(0).asMap().get("key"));
        Assertions.assertEquals(2L, records.get(0).asMap().get("total"));
        List<Object> groupKeys = GroupFeature.getGroupKey(records.get(0));
        Assertions.assertEquals(1, groupKeys.size());
        Assertions.assertSame(key, groupKeys.get(0));
    }

    @Test
    void shouldPreserveDemandCancellationContextAndError() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean contextVisible = new AtomicBoolean();
        AtomicInteger visited = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                                   .sql("select key,count(1) total from test group by _window(2),key")
                                   .build();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.deferContextual(view -> {
            contextVisible.set(view.hasKey(ReactorQLContext.class));
            return Flux.range(0, 1000)
                       .doOnNext(value -> visited.incrementAndGet())
                       .map(value -> row("key-" + (value & 1), value))
                       .doOnCancel(() -> cancelled.set(true));
        }));

        StepVerifier.create(query.start(context), 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .assertNext(record -> Assertions.assertEquals(1L, record.asMap().get("total")))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(contextVisible.get());
        Assertions.assertTrue(cancelled.get());
        Assertions.assertTrue(visited.get() < 1000);

        StepVerifier.create(query.start(Flux.concat(
                        Flux.just(row("a", 1)),
                        Flux.error(new IllegalStateException("source failed")))))
                    .expectErrorMessage("source failed")
                    .verify();
    }

    @Test
    void shouldFallbackForCustomFromAndPropertyAndMultipleDimensions() {
        String sql = "select key,count(1) total from test group by _window(2),key";
        DefaultPropertyFeature customProperty = new DefaultPropertyFeature() {
            @Override
            public Optional<Object> getProperty(Object property, Object source) {
                return "key".equals(property) ? Optional.of("forced") : super.getProperty(property, source);
            }
        };
        ReactorQL propertyQuery = ReactorQL.builder().feature(customProperty).sql(sql).build();
        Assertions.assertTrue(((DefaultReactorQL) propertyQuery).describeExecutionPlan()
                                                                   .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(propertyQuery.start(Flux.just(row("a", 1), row("b", 2))))
                    .assertNext(result -> Assertions.assertEquals("forced", result.get("key")))
                    .verifyComplete();

        AtomicInteger subscriptions = new AtomicInteger();
        FromFeature customFrom = new FromFeature() {
            @Override
            public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(
                    FromItem fromItem,
                    ReactorQLMetadata metadata) {
                return context -> Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.just(row("a", 1), row("a", 2))
                               .map(value -> ReactorQLRecord.newRecord("test", value, context));
                });
            }

            @Override
            public String getId() {
                return FeatureId.From.table.getId();
            }
        };
        ReactorQL fromQuery = ReactorQL.builder().feature(customFrom).sql(sql).build();
        Assertions.assertTrue(((DefaultReactorQL) fromQuery).describeExecutionPlan()
                                                               .contains("ASYNC_OR_STATEFUL[projection]"));
        StepVerifier.create(fromQuery.start(new DefaultReactorQLContext(ignore -> Flux.empty())))
                    .assertNext(record -> Assertions.assertEquals(2L, record.asMap().get("total")))
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());

        ReactorQL twoDimensions = ReactorQL.builder()
                                          .sql("select key,count(1) total from test group by key,_group_by_key")
                                          .build();
        Assertions.assertTrue(((DefaultReactorQL) twoDimensions).describeExecutionPlan()
                                                                    .contains("ASYNC_OR_STATEFUL[projection]"));

        ReactorQL customGroup = ReactorQL.builder()
                                        .feature(new GroupByValueFeature("property") {})
                                        .sql("select product,device,count(1) total from test "
                                                     + "group by product,device")
                                        .build();
        Assertions.assertTrue(((DefaultReactorQL) customGroup).describeExecutionPlan()
                                                                  .contains("ASYNC_OR_STATEFUL[projection]"));
    }

    @Test
    void shouldMatchRecordPathForCompositeKeysAcrossWindowPositions() {
        List<Map<String, Object>> rows = Arrays.asList(
                compositeRow("a", "x", 1), compositeRow("b", "x", 2),
                compositeRow("a", null, 3), compositeRow("a", "y", 4),
                compositeRow(null, "x", 5), compositeRow("a", "x", 6),
                compositeRow("b", "x", 7), compositeRow("a", "y", 8),
                compositeRow("a", "x", 9));
        String columns = "select product,device,count(1) total,sum(score) sum,"
                + "avg(score) avg,max(score) max from test ";
        String[] sqls = {
                columns + "group by product,device",
                columns + "group by _window(4),product,device",
                columns + "group by product,_window(3),device",
                columns + "group by product,device,_window(2)",
                columns + "group by _window(4),product,device having total > 1"
        };
        for (String sql : sqls) {
            ReactorQL raw = ReactorQL.builder().sql(sql).build();
            ReactorQL record = ReactorQL.builder()
                                             .feature(new DefaultPropertyFeature())
                                             .sql(sql)
                                             .build();
            Assertions.assertTrue(((DefaultReactorQL) raw).describeExecutionPlan()
                                                         .contains("ASYNC_OR_STATEFUL[projection]"), sql);
            Assertions.assertTrue(((DefaultReactorQL) record).describeExecutionPlan()
                                                            .contains("ASYNC_OR_STATEFUL[projection]"), sql);
            Assertions.assertEquals(record.start(Flux.fromIterable(rows)).collectList().block(),
                                    raw.start(Flux.fromIterable(rows)).collectList().block(), sql);
        }
    }

    @Test
    void shouldUpgradeMiddleWindowSuffixGroupsWithoutChangingOrderOrBudget() {
        String sql = "select product,device,region,count(1) total from test "
                + "group by product,_window(4),device,region";
        List<Map<String, Object>> rows = Arrays.asList(
                compositeRowWithRegion("a", "x", "north", 1),
                compositeRowWithRegion("a", "x", "north", 2),
                compositeRowWithRegion("a", "y", "south", 3),
                compositeRowWithRegion("a", "x", "north", 4),
                compositeRowWithRegion("b", "z", "north", 5));
        ReactorQL raw = ReactorQL.builder().sql(sql).build();
        ReactorQL record = ReactorQL.builder()
                                    .feature(new DefaultPropertyFeature())
                                    .sql(sql)
                                    .build();
        ReactorQL publisher = ReactorQL.builder()
                                       .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                       .sql(sql)
                                       .build();
        List<Map<String, Object>> actual = raw.start(Flux.fromIterable(rows)).collectList().block();
        Assertions.assertEquals(record.start(Flux.fromIterable(rows)).collectList().block(), actual);
        Assertions.assertEquals(publisher.start(Flux.fromIterable(rows)).collectList().block(), actual);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(3, actual.size());
        Assertions.assertEquals("x", actual.get(0).get("device"));
        Assertions.assertEquals(3L, actual.get(0).get("total"));
        Assertions.assertEquals("y", actual.get(1).get("device"));
        Assertions.assertEquals(1L, actual.get(1).get("total"));
        Assertions.assertEquals("z", actual.get(2).get("device"));

        ReactorQL bounded = ReactorQL.builder()
                                     .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 1)
                                     .sql(sql)
                                     .build();
        StepVerifier.create(bounded.start(Flux.fromIterable(rows)).collectList())
                    .expectErrorMatches(error -> error instanceof org.jetlinks.reactor.ql.exception.ReactorQLException
                            && error.getMessage().contains(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS))
                    .verify();
    }

    @Test
    void shouldCancelMiddleWindowWithPendingSuffixGroup() {
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactorQL query = ReactorQL.builder()
                                   .sql("select product,device,count(1) total from test "
                                                + "group by product,_window(4),device")
                                   .build();
        Flux<Map<String, Object>> source = Flux.range(0, 100)
                                               .map(index -> compositeRow("a", index % 4 == 1 ? "y" : "x", index))
                                               .doOnCancel(() -> cancelled.set(true));
        StepVerifier.create(query.start(source), 0)
                    .thenRequest(1)
                    .assertNext(row -> {
                        Assertions.assertEquals("x", row.get("device"));
                        Assertions.assertEquals(3L, row.get("total"));
                    })
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void shouldPreserveCompositeGroupKeysAndRecordFallback() {
        String sql = "select product,device,count(1) total from test group by product,device";
        ReactorQL query = ReactorQL.builder().sql(sql).build();
        ReactorQL recordPath = ReactorQL.builder()
                                       .feature(new DefaultPropertyFeature())
                                       .sql(sql)
                                       .build();
        Flux<Object> source = Flux.defer(() -> {
            ReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
            return Flux.just(compositeRow("a", "x", 1),
                             ReactorQLRecord.newRecord("upstream", compositeRow("a", "x", 2), context),
                             compositeRow("b", "y", 3));
        });
        Assertions.assertEquals(recordPath.start(source).collectList().block(),
                                query.start(source).collectList().block());
        List<ReactorQLRecord> output = query.start(new DefaultReactorQLContext(ignore -> source))
                                            .collectList().block();
        Assertions.assertNotNull(output);
        Assertions.assertEquals(2, output.size());
        List<Object> firstKeys = GroupFeature.getGroupKey(output.get(0));
        Assertions.assertEquals(Arrays.asList("a", "x"), firstKeys);
        Assertions.assertEquals(Arrays.asList("b", "y"), GroupFeature.getGroupKey(output.get(1)));
        firstKeys.add("consumer-change");
        Assertions.assertEquals(Arrays.asList("a", "x"), GroupFeature.getGroupKey(output.get(0)));
    }

    @Test
    void shouldPreserveCompositeDemandCancellationContextAndBudget() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean contextVisible = new AtomicBoolean();
        AtomicInteger visited = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                                   .sql("select product,device,count(1) total from test "
                                                + "group by _window(2),product,device")
                                   .build();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.deferContextual(view -> {
            contextVisible.set(view.hasKey(ReactorQLContext.class));
            return Flux.range(0, 1000)
                       .doOnNext(value -> visited.incrementAndGet())
                       .map(value -> compositeRow("a", "x", value))
                       .doOnCancel(() -> cancelled.set(true));
        }));
        StepVerifier.create(query.start(context), 0)
                    .thenRequest(1)
                    .assertNext(record -> Assertions.assertEquals(2L, record.asMap().get("total")))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(contextVisible.get());
        Assertions.assertTrue(cancelled.get());
        Assertions.assertTrue(visited.get() < 1000);

        StepVerifier.create(query.start(Flux.concat(
                        Flux.just(compositeRow("a", "x", 1)),
                        Flux.error(new IllegalStateException("source failed")))))
                    .expectErrorMessage("source failed")
                    .verify();

        ReactorQL bounded = ReactorQL.builder()
                                     .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, 4)
                                     .sql("select product,device,count(1) total from test "
                                                  + "group by product,device")
                                     .build();
        StepVerifier.create(bounded.start(Flux.just(compositeRow("a", "x", 1),
                                                    compositeRow("a", "y", 2),
                                                    compositeRow("b", "x", 3),
                                                    compositeRow("b", "y", 4),
                                                    compositeRow("c", "x", 5))))
                    .expectErrorMatches(error -> error instanceof org.jetlinks.reactor.ql.exception.ReactorQLException
                            && error.getMessage().contains(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS))
                    .verify();
    }

    private static Map<String, Object> compositeRow(String product, String device, int score) {
        Map<String, Object> row = new HashMap<>();
        row.put("product", product);
        row.put("device", device);
        row.put("score", score);
        return row;
    }

    private static Map<String, Object> compositeRowWithRegion(String product,
                                                               String device,
                                                               String region,
                                                               int score) {
        Map<String, Object> row = compositeRow(product, device, score);
        row.put("region", region);
        return row;
    }

    private static Flux<Object> mixedSource() {
        return Flux.defer(() -> {
            ReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
            List<Object> rows = new ArrayList<>();
            rows.add(row("a", 1));
            rows.add(ReactorQLRecord.newRecord("upstream", row("a", 2), context));
            rows.add(row("b", 3));
            rows.add(ReactorQLRecord.newRecord("upstream", row("b", 4), context));
            return Flux.fromIterable(rows);
        });
    }

    private static Map<String, Object> row(String key, int score) {
        Map<String, Object> row = new HashMap<>();
        row.put("key", key);
        row.put("score", score);
        return row;
    }

    private static Map<Map<String, Object>, Integer> multiset(List<Map<String, Object>> rows) {
        Assertions.assertNotNull(rows);
        Map<Map<String, Object>, Integer> result = new HashMap<>();
        rows.forEach(row -> result.merge(row, 1, Integer::sum));
        return result;
    }
}
