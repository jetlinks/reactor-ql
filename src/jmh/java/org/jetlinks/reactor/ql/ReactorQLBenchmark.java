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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.jetlinks.reactor.ql.utils.CalculateUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import org.reactivestreams.Publisher;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class ReactorQLBenchmark {

    private static final int LARGE_ROWS = 1_000_000;
    private static final int FUNCTION_ROWS = 20_000;
    private static final int HIGH_CARDINALITY_ROWS = 50_000;
    private static final int SORT_ROWS = 5_000;
    private static final int MULTI_VALUE_ROWS = 1_024;
    private static final int JOIN_PROFILE_LEFT_ROWS = 20_000;
    private static final int JOIN_PROFILE_RIGHT_ROWS = 21;
    private static final int JOIN_PROFILE_RESULT_ROWS = 105_000;

    private ReactorQL count;
    private ReactorQL globalAggregates;
    private ReactorQL filteredGlobalAggregates;
    private ReactorQL globalAggregatesPublisher;
    private ReactorQL where;
    private ReactorQL projection;
    private ReactorQL wideProjection;
    private ReactorQL starProjection;
    private ReactorQL commonFunctions;
    private ReactorQL regexpLikeLiteral;
    private ReactorQL regexpLikeDynamic;
    private ReactorQL jsonPath;
    private ReactorQL profilingJsonGet;
    private ReactorQL profilingColdJsonGet;
    private ReactorQL profilingAsyncStarProjection;
    private ReactorQL profilingAsyncProjection;
    private ReactorQL profilingSyncTableStarProjection;
    private ReactorQL windowAggregates;
    private ReactorQL windowAggregatesPublisher;
    private ReactorQL collectRows;
    private ReactorQL collectRowsPublisher;
    private ReactorQL subquery;
    private ReactorQL twoAsyncSubqueries;
    private ReactorQL threeAsyncSubqueries;
    private ReactorQL nestedSubquery;
    private ReactorQL deeplyNestedSubquery;
    private ReactorQL subqueryUncached;
    private ReactorQL existsSubquery;
    private ReactorQL existsSubqueryUncached;
    private ReactorQL highCardinalityAggregates;
    private ReactorQL highCardinalityCount;
    private ReactorQL highCardinalityPerKeyWindowCount;
    private ReactorQL innerJoin;
    private ReactorQL profilingMultiRowInnerJoin;
    private ReactorQL profilingAsyncOnMultiRowInnerJoin;
    private ReactorQL orderBy;
    private ReactorQL orderByLimit;
    private ReactorQL singleAsyncOrderByLimit;
    private ReactorQL doubleAsyncOrderByLimit;
    private ReactorQL distinctRows;
    private ReactorQL intersectRows;
    private ReactorQL unionRows;
    private ReactorQL unionAllRows;
    private ReactorQL exceptRows;
    private ReactorQL leftJoin;
    private ReactorQL rightJoin;
    private ReactorQL correlatedSubquery;
    private ReactorQL publisherFeature;
    private ReactorQL singleArgumentPublisherFunction;
    private ReactorQL twoArgumentPublisherFunction;
    private ReactorQL multiValueAggregates;

    private Flux<Integer> largeSource;
    private volatile Flux<Integer> wideProjectionInput;
    private volatile boolean profilingProjectionInputValidated;
    private volatile boolean profilingWhereInputValidated;
    private Flux<Map<String, Object>> commonFunctionSource;
    private Flux<Map<String, Object>> regexpSource;
    private Flux<Map<String, Object>> regexpAlternatingSource;
    private Flux<Map<String, Object>> jsonSource;
    private Flux<Map<String, Object>> profilingJsonGetSource;
    private AtomicInteger profilingColdJsonSubscriptions;
    private Flux<Map<String, Object>> profilingAsyncStarSource;
    private AtomicInteger profilingAsyncStarSubscriptions;
    private Flux<Map<String, Object>> groupedSource;
    private volatile Flux<Map<String, Object>> profilingGroupedSource;
    private Map<String, Object>[] groupedRows;
    private Flux<Map<String, Object>> highCardinalitySource;
    private volatile Flux<Map<String, Object>> profilingHighCardinalitySource;
    private Flux<Map<String, Object>> joinLeftSource;
    private Flux<Map<String, Object>> joinRightSource;
    private Map<String, Object> joinRightRow;
    private Function<String, Publisher<?>> subquerySource;
    private Function<String, Publisher<?>> joinSource;
    private Function<String, Publisher<?>> profilingMultiRowJoinSource;
    private AtomicInteger profilingMultiRowRightSubscriptions;
    private AtomicInteger profilingAsyncOnSubscriptions;
    private Function<String, Publisher<?>> setSource;
    private Function<String, Publisher<?>> profilingSetSource;
    private Flux<Map<String, Object>> setLeftSource;
    private Flux<Map<String, Object>> setRightSource;
    private AtomicInteger profilingSetLeftSubscriptions;
    private AtomicInteger profilingSetRightSubscriptions;
    private Function<String, Publisher<?>> correlatedSource;
    private Flux<Integer> sortedInput;
    private Integer[] profilingOrderByRows;
    private Integer[] profilingAscendingOrderByRows;
    private Integer[] profilingMixedOrderByRows;
    private AtomicInteger singleAsyncOrderKeySubscriptions;
    private AtomicInteger firstAsyncOrderKeySubscriptions;
    private AtomicInteger secondAsyncOrderKeySubscriptions;
    private Flux<Integer> distinctInput;
    private Flux<Integer> profilingDistinctInput;
    private Flux<Map<String, Object>> publisherInput;
    private Flux<Integer> multiValueInput;
    private ReactorQLContext nativeRecordContext;
    private Flux<Map<String, Object>> completedReplayCache;
    private Flux<Map<String, Object>> completedSnapshotPublisher;
    private List<Map<String, Object>> completedCacheRows;

    @Setup
    public void setup() {
        count = ReactorQL.builder().sql("select count(1) total from test").build();
        String globalSql = "select count(1) total,sum(score) sum,avg(score) avg,"
                + "min(score) min,max(score) max from test";
        globalAggregates = ReactorQL.builder().sql(globalSql).build();
        String filteredGlobalSql = globalSql + " where score >= 0 and score < 512";
        filteredGlobalAggregates = ReactorQL.builder().sql(filteredGlobalSql).build();
        globalAggregatesPublisher = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(globalSql)
                .build();
        where = ReactorQL
                .builder()
                .sql("select count(1) total from test where this >= 0 and this < 1000000")
                .build();
        projection = ReactorQL
                .builder()
                .sql("select this + 1 next_value, this * 2 doubled from test " +
                             "where this >= 0 and this < 1000000")
                .build();
        wideProjection = ReactorQL
                .builder()
                .sql("select this value,this + 1 plus,this * 2 doubled,this - 3 difference,"
                             + "this % 7 remainder,this / 2 half,this >= 0 non_negative,"
                             + "this < 500000 first_half from test")
                .build();
        starProjection = ReactorQL.builder().sql("select * from test").build();
        commonFunctions = ReactorQL
                .builder()
                .sql("select count(split_part(text, ',', 2)) total from test " +
                             "where str_contains(text, 'beta') " +
                             "and date_diff(date_add(time, 1, 'day'), time, 'day') = 1")
                .build();
        regexpLikeLiteral = ReactorQL.builder()
                                     .sql("select regexp_like(text, '^alpha,beta,gamma$') matched from test")
                                     .build();
        regexpLikeDynamic = ReactorQL.builder()
                                     .sql("select regexp_like(text, regex) matched from test")
                                     .build();
        jsonPath = ReactorQL
                .builder()
                .sql("select count(json_get(json, '$.point.lon')) total from test")
                .build();
        profilingJsonGet = ReactorQL.builder()
                                    .sql("select json_get(json, '$.point.lon') lon from test")
                                    .build();
        profilingColdJsonSubscriptions = new AtomicInteger();
        ValueMapFeature coldJson = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.defer(() -> {
                    profilingColdJsonSubscriptions.incrementAndGet();
                    return Mono.justOrEmpty(((Map<?, ?>) record.getRecord()).get("json"));
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_json").getId();
            }
        };
        profilingColdJsonGet = ReactorQL.builder()
                                        .feature(coldJson)
                                        .sql("select json_get(cold_json(json), '$.point.lon') lon from test")
                                        .build();
        profilingAsyncStarSubscriptions = new AtomicInteger();
        ValueMapFeature profilingAsyncStarValue = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.deferContextual(context -> {
                    if (!"async-star".equals(context.getOrDefault("profiling-context", null))) {
                        return Mono.error(new IllegalStateException("异步星号 JFR 输入丢失 Reactor Context"));
                    }
                    profilingAsyncStarSubscriptions.incrementAndGet();
                    return Mono.just(((Number) ((Map<?, ?>) record.getRecord()).get("id")).longValue() + 1);
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("profiling_async_star_value").getId();
            }
        };
        profilingAsyncStarProjection = ReactorQL.builder()
                                                .feature(profilingAsyncStarValue)
                                                .sql("select t.*,profiling_async_star_value(t.id) async_id from test t")
                                                .build();
        profilingAsyncProjection = ReactorQL.builder()
                                            .feature(profilingAsyncStarValue)
                                            .sql("select t.id AS id,t.name AS name,t.score AS score,profiling_async_star_value(t.id) async_id "
                                                         + "from test t")
                                            .build();
        profilingSyncTableStarProjection = ReactorQL.builder()
                                                     .sql("select t.* from test t")
                                                     .build();
        String groupedSql = "select type,count(1) total,sum(score) sum,avg(score) avg,"
                + "min(score) min,max(score) max from test group by _window(10000),type";
        windowAggregates = ReactorQL.builder().sql(groupedSql).build();
        windowAggregatesPublisher = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(groupedSql)
                .build();
        String collectRowsSql = "select collect_row(type,score) rows "
                + "from test group by _window(10000)";
        collectRows = ReactorQL.builder().sql(collectRowsSql).build();
        collectRowsPublisher = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(collectRowsSql)
                .build();
        String subquerySql = "select id,(select value from lookup) cached from outer_table";
        subquery = ReactorQL.builder().sql(subquerySql).build();
        twoAsyncSubqueries = ReactorQL
                .builder()
                .sql("select id,(select value from lookup) first_cached,"
                             + "(select value from lookup) second_cached from outer_table")
                .build();
        threeAsyncSubqueries = ReactorQL
                .builder()
                .sql("select id,(select value from lookup) first_cached,"
                             + "(select value from lookup) second_cached,"
                             + "(select value from lookup) third_cached from outer_table")
                .build();
        nestedSubquery = ReactorQL
                .builder()
                .sql("select o.id,(select n.value from (select value from lookup) n) cached "
                             + "from outer_table o")
                .build();
        deeplyNestedSubquery = ReactorQL
                .builder()
                .sql("select o.id,(select n2.value from ("
                             + "select n1.value AS value from (select value from lookup) n1"
                             + ") n2) cached from outer_table o")
                .build();
        subqueryUncached = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, false)
                .sql(subquerySql)
                .build();
        String existsSql = "select id from outer_table "
                + "where exists(select value from lookup)";
        existsSubquery = ReactorQL.builder().sql(existsSql).build();
        existsSubqueryUncached = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, false)
                .sql(existsSql)
                .build();
        highCardinalityAggregates = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, HIGH_CARDINALITY_ROWS)
                .sql("select key,count(1) total,sum(score) sum,avg(score) avg "
                             + "from test group by _window(50000),key")
                .build();
        highCardinalityCount = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, HIGH_CARDINALITY_ROWS)
                .sql("select key,count(1) total from test group by _window(50000),key")
                .build();
        highCardinalityPerKeyWindowCount = ReactorQL
                .builder()
                .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, HIGH_CARDINALITY_ROWS * 2)
                .sql("select key,count(1) total from test group by key,_window(2)")
                .build();
        innerJoin = ReactorQL
                .builder()
                .sql("select t1.v value from t1 join t2 on t1.v = t2.v")
                .build();
        profilingMultiRowInnerJoin = ReactorQL
                .builder()
                .sql("select t1.key left_key,t2.key right_key from t1 join t2 on t1.key = t2.key")
                .build();
        profilingAsyncOnSubscriptions = new AtomicInteger();
        profilingAsyncOnMultiRowInnerJoin = ReactorQL
                .builder()
                .feature(coldJoinOnKey("cold_join_key", profilingAsyncOnSubscriptions))
                .sql("select t1.key left_key,t2.key right_key from t1 join t2 "
                             + "on t1.key = cold_join_key(t2.key)")
                .build();
        orderBy = ReactorQL.builder()
                           .sql("select this val from test order by this")
                           .build();
        orderByLimit = ReactorQL.builder()
                                .sql("select this val from test order by this limit 100")
                                .build();
        singleAsyncOrderKeySubscriptions = new AtomicInteger();
        firstAsyncOrderKeySubscriptions = new AtomicInteger();
        secondAsyncOrderKeySubscriptions = new AtomicInteger();
        singleAsyncOrderByLimit = ReactorQL.builder()
                                            .feature(coldOrderByKey("cold_order_key",
                                                                    singleAsyncOrderKeySubscriptions))
                                            .sql("select this val from test "
                                                         + "order by cold_order_key(this) limit 100")
                                            .build();
        doubleAsyncOrderByLimit = ReactorQL.builder()
                                            .feature(coldOrderByKey("first_cold_order_key",
                                                                    firstAsyncOrderKeySubscriptions),
                                                     coldOrderByKey("second_cold_order_key",
                                                                    secondAsyncOrderKeySubscriptions))
                                            .sql("select this val from test order by "
                                                         + "first_cold_order_key(this), "
                                                         + "second_cold_order_key(this) limit 100")
                                            .build();
        distinctRows = ReactorQL.builder()
                                .sql("select distinct this val from test")
                                .build();
        intersectRows = ReactorQL.builder()
                                  .sql("select s.v from (select v from t1 intersect select v from t2) s")
                                  .build();
        unionRows = ReactorQL.builder()
                             .sql("select s.v from (select v from t1 union select v from t2) s")
                             .build();
        unionAllRows = ReactorQL.builder()
                                .sql("select s.v from (select v from t1 union all select v from t2) s")
                                .build();
        exceptRows = ReactorQL.builder()
                                .sql("select s.v from (select v from t1 except select v from t2) s")
                                .build();
        leftJoin = ReactorQL.builder()
                            .sql("select t1.v value from t1 left join t2 on t1.v = t2.v")
                            .build();
        rightJoin = ReactorQL.builder()
                             .sql("select t2.v value from t1 right join t2 on t1.v = t2.v")
                             .build();
        correlatedSubquery = ReactorQL.builder()
                                      .sql("select o.id,(select value from lookup where lookup.id=o.id) value "
                                                   + "from outer_table o")
                                      .build();
        ValueMapFeature coldPublisher = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.defer(() -> Mono.justOrEmpty(
                        ((Map<?, ?>) record.getRecord()).get("id")));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("cold_value").getId();
            }
        };
        publisherFeature = ReactorQL.builder()
                                    .feature(coldPublisher)
                                    .sql("select cold_value(id) value from outer_table")
                                    .build();
        singleArgumentPublisherFunction = ReactorQL.builder()
                .feature(coldPublisher,
                         new FunctionMapFeature("pass_through", 1, 1, Flux::next))
                .sql("select pass_through(cold_value(id)) value from outer_table")
                .build();
        twoArgumentPublisherFunction = ReactorQL.builder()
                .feature(coldPublisher,
                         new FunctionMapFeature("sum_pair", 2, 2, values -> values.collectList()
                                 .map(arguments -> ((Number) arguments.get(0)).intValue()
                                         + ((Number) arguments.get(1)).intValue())))
                .sql("select sum_pair(cold_value(id),cold_value(id)) value from outer_table")
                .build();
        ValueAggMapFeature emitEach = new ValueAggMapFeature() {
            @Override
            public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression,
                                                                                ReactorQLMetadata metadata) {
                return rows -> rows.map(ReactorQLRecord::getRecord);
            }

            @Override
            public String getId() {
                return FeatureId.ValueAggMap.of("emit_each").getId();
            }
        };
        multiValueAggregates = ReactorQL.builder()
                                        .feature(emitEach)
                                        .sql("select emit_each(this) emitted,count(1) total from test")
                                        .build();
        multiValueInput = Flux.range(0, MULTI_VALUE_ROWS);

        largeSource = Flux.range(0, LARGE_ROWS);
        Map<String, Object> commonRow = new HashMap<>();
        commonRow.put("text", "alpha,beta,gamma");
        commonRow.put("time", "2024-02-01 00:00:00");
        commonFunctionSource = Flux.range(0, FUNCTION_ROWS).map(ignore -> commonRow);
        Map<String, Object> regexpRow = new HashMap<>(commonRow);
        regexpRow.put("regex", "^alpha,beta,gamma$");
        @SuppressWarnings("unchecked")
        Map<String, Object>[] regexpRows = new Map[FUNCTION_ROWS];
        java.util.Arrays.fill(regexpRows, regexpRow);
        regexpSource = Flux.fromArray(regexpRows);
        Map<String, Object> alternateRegexpRow = new HashMap<>(commonRow);
        alternateRegexpRow.put("regex", "^alpha,.*gamma$");
        @SuppressWarnings("unchecked")
        Map<String, Object>[] alternatingRegexpRows = new Map[FUNCTION_ROWS];
        for (int i = 0; i < alternatingRegexpRows.length; i++) {
            alternatingRegexpRows[i] = (i & 1) == 0 ? regexpRow : alternateRegexpRow;
        }
        regexpAlternatingSource = Flux.fromArray(alternatingRegexpRows);
        assertRegexpLikeResult(regexpLikeLiteral.start(regexpSource), "字面量正则 JFR 输入");
        assertRegexpLikeResult(regexpLikeDynamic.start(regexpSource), "动态正则 JFR 输入");
        assertRegexpLikeResult(regexpLikeDynamic.start(regexpAlternatingSource), "交替正则 JFR 输入");
        Map<String, Object> jsonRow = Collections.singletonMap(
                "json",
                "{\"point\":{\"lon\":120.12,\"lat\":30.16},\"tags\":[\"a\",\"b\",\"c\"]}"
        );
        jsonSource = Flux.range(0, FUNCTION_ROWS).map(ignore -> jsonRow);
        @SuppressWarnings("unchecked")
        Map<String, Object>[] jsonRows = new Map[FUNCTION_ROWS];
        java.util.Arrays.fill(jsonRows, jsonRow);
        profilingJsonGetSource = Flux.fromArray(jsonRows);
        assertProfilingJsonGetResult(profilingJsonGet.start(profilingJsonGetSource));
        profilingColdJsonSubscriptions.set(0);
        assertProfilingJsonGetResult(profilingColdJsonGet.start(profilingJsonGetSource));
        if (profilingColdJsonSubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException("冷 JSON 参数未逐行订阅: "
                                                    + profilingColdJsonSubscriptions.get());
        }
        @SuppressWarnings("unchecked")
        Map<String, Object>[] asyncStarRows = new Map[FUNCTION_ROWS];
        for (int index = 0; index < asyncStarRows.length; index++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", index);
            row.put("name", "name-" + (index & 15));
            row.put("score", index & 255);
            asyncStarRows[index] = row;
        }
        profilingAsyncStarSource = Flux.fromArray(asyncStarRows);
        profilingAsyncStarSubscriptions.set(0);
        assertProfilingAsyncStarResult(profilingAsyncStarProjection.start(profilingAsyncStarSource)
                                                                  .contextWrite(context -> context.put("profiling-context", "async-star")),
                                        true,
                                        true,
                                        "异步星号 JFR 输入");
        assertAsyncStarSubscriptions("异步星号 JFR 输入");
        profilingAsyncStarSubscriptions.set(0);
        assertProfilingAsyncStarResult(profilingAsyncProjection.start(profilingAsyncStarSource)
                                                              .contextWrite(context -> context.put("profiling-context", "async-star")),
                                        false,
                                        true,
                                        "异步无星号 JFR 输入");
        assertAsyncStarSubscriptions("异步无星号 JFR 输入");
        assertProfilingAsyncStarResult(profilingSyncTableStarProjection.start(profilingAsyncStarSource),
                                        true,
                                        false,
                                        "同步表星号 JFR 输入");
        @SuppressWarnings("unchecked")
        Map<String, Object>[] groupedRows = new Map[1024];
        for (int i = 0; i < groupedRows.length; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("type", "type-" + (i & 31));
            row.put("score", i);
            groupedRows[i] = row;
        }
        this.groupedRows = groupedRows;
        groupedSource = Flux.range(0, LARGE_ROWS).map(index -> groupedRows[index & 1023]);
        Map<String, Object> filteredResult = filteredGlobalAggregates.start(groupedSource).single().block();
        Map<String, Object> publisherFilteredResult = ReactorQL.builder()
                .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                .sql(filteredGlobalSql)
                .build()
                .start(groupedSource)
                .single()
                .block();
        if (!java.util.Objects.equals(filteredResult, publisherFilteredResult)) {
            throw new IllegalStateException("过滤聚合基准与 Publisher 结果不一致");
        }
        highCardinalitySource = Flux
                .range(0, HIGH_CARDINALITY_ROWS)
                .map(index -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("key", "key-" + index);
                    row.put("score", index);
                    // 宽 payload 不参与查询，用于观测分组状态是否错误保留整行。
                    row.put("payload", new byte[1024]);
                    return row;
                });
        nativeRecordContext = new DefaultReactorQLContext(ignore -> Flux.empty());
        @SuppressWarnings("unchecked")
        Map<String, Object>[] outerRows = new Map[1024];
        for (int i = 0; i < outerRows.length; i++) {
            outerRows[i] = Collections.<String, Object>singletonMap("id", i);
        }
        Flux<Map<String, Object>> outerSource = Flux
                .range(0, FUNCTION_ROWS)
                .map(index -> outerRows[index & 1023]);
        Flux<Map<String, Object>> lookupSource = Flux.just(
                Collections.<String, Object>singletonMap("value", 1)
        );
        subquerySource = name -> "lookup".equals(name) ? lookupSource : outerSource;
        AtomicInteger nestedLookupSubscriptions = new AtomicInteger();
        Function<String, Publisher<?>> countedSubquerySource = name -> {
            if ("lookup".equals(name)) {
                return lookupSource.doOnSubscribe(ignore -> nestedLookupSubscriptions.incrementAndGet());
            }
            return outerSource;
        };
        assertNestedSubqueryResult(nestedSubquery.start(countedSubquerySource), "n.value", "两层子查询");
        if (nestedLookupSubscriptions.get() != 1) {
            throw new IllegalStateException("两层子查询未复用 lookup 结果");
        }
        nestedLookupSubscriptions.set(0);
        assertNestedSubqueryResult(deeplyNestedSubquery.start(countedSubquerySource), "n2.value", "三层子查询");
        if (nestedLookupSubscriptions.get() != 1) {
            throw new IllegalStateException("三层子查询未复用 lookup 结果");
        }
        completedCacheRows = new ArrayList<>(Collections.singletonList(
                Collections.<String, Object>singletonMap("value", 1)));
        completedSnapshotPublisher = Flux.fromIterable(completedCacheRows);
        AtomicInteger cacheSourceSubscriptions = new AtomicInteger();
        completedReplayCache = Mono
                .defer(() -> {
                    cacheSourceSubscriptions.incrementAndGet();
                    return Mono.just(completedCacheRows);
                })
                .flux()
                .replay(1)
                .refCount(1)
                .singleOrEmpty()
                .flatMapIterable(values -> values);
        completedReplayCache.collectList().block();
        long replay = consume(Flux.range(0, 3).flatMap(ignore -> completedReplayCache));
        long snapshot = consume(Flux.range(0, 3)
                                    .flatMap(ignore -> Flux.fromIterable(completedCacheRows)));
        long publisher = consume(Flux.range(0, 3)
                                     .flatMap(ignore -> completedSnapshotPublisher));
        if (replay != snapshot || replay != publisher || cacheSourceSubscriptions.get() != 1) {
            throw new IllegalStateException("已完成缓存读取基准的结果或共享语义不一致");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object>[] joinRows = new Map[2];
        joinRows[0] = Collections.<String, Object>singletonMap("v", 0);
        joinRows[1] = Collections.<String, Object>singletonMap("v", 1);
        joinLeftSource = Flux.range(0, FUNCTION_ROWS).map(index -> joinRows[index & 1]);
        joinRightRow = Collections.<String, Object>singletonMap("v", 0);
        joinRightSource = Flux.just(joinRightRow);
        joinSource = name -> "t2".equals(name) ? joinRightSource : joinLeftSource;
        @SuppressWarnings("unchecked")
        Map<String, Object>[] profilingJoinLeftRows = new Map[JOIN_PROFILE_LEFT_ROWS];
        for (int index = 0; index < profilingJoinLeftRows.length; index++) {
            profilingJoinLeftRows[index] = Collections.<String, Object>singletonMap("key", index & 3);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object>[] profilingJoinRightRows = new Map[JOIN_PROFILE_RIGHT_ROWS];
        int rightIndex = 0;
        for (int key = 1; key <= 3; key++) {
            int copies = 1 << ((key - 1) * 2);
            for (int copy = 0; copy < copies; copy++) {
                profilingJoinRightRows[rightIndex++] = Collections.<String, Object>singletonMap("key", key);
            }
        }
        if (rightIndex != JOIN_PROFILE_RIGHT_ROWS) {
            throw new IllegalStateException("多行 JOIN JFR 右源行数不符合前置条件: " + rightIndex);
        }
        profilingMultiRowRightSubscriptions = new AtomicInteger();
        profilingMultiRowJoinSource = name -> "t2".equals(name)
                ? Flux.defer(() -> {
                    profilingMultiRowRightSubscriptions.incrementAndGet();
                    return Flux.fromArray(profilingJoinRightRows);
                })
                : Flux.fromArray(profilingJoinLeftRows);
        profilingMultiRowRightSubscriptions.set(0);
        assertProfilingMultiRowJoinResult(profilingMultiRowInnerJoin.start(profilingMultiRowJoinSource),
                                           "同步多行 INNER JOIN JFR 输入");
        if (profilingMultiRowRightSubscriptions.get() != JOIN_PROFILE_LEFT_ROWS) {
            throw new IllegalStateException("多行 INNER JOIN 未逐左行订阅右源: "
                                                    + profilingMultiRowRightSubscriptions.get());
        }
        profilingMultiRowRightSubscriptions.set(0);
        profilingAsyncOnSubscriptions.set(0);
        assertProfilingMultiRowJoinResult(profilingAsyncOnMultiRowInnerJoin.start(profilingMultiRowJoinSource),
                                           "异步 ON 多行 INNER JOIN JFR 输入");
        if (profilingMultiRowRightSubscriptions.get() != JOIN_PROFILE_LEFT_ROWS) {
            throw new IllegalStateException("异步 ON 多行 INNER JOIN 未逐左行订阅右源: "
                                                    + profilingMultiRowRightSubscriptions.get());
        }
        int expectedAsyncOnSubscriptions = JOIN_PROFILE_LEFT_ROWS * JOIN_PROFILE_RIGHT_ROWS;
        if (profilingAsyncOnSubscriptions.get() != expectedAsyncOnSubscriptions) {
            throw new IllegalStateException("异步 ON 未逐候选订阅: " + profilingAsyncOnSubscriptions.get());
        }
        sortedInput = Flux.range(0, FUNCTION_ROWS).map(index -> FUNCTION_ROWS - index - 1);
        profilingOrderByRows = new Integer[FUNCTION_ROWS];
        profilingAscendingOrderByRows = new Integer[FUNCTION_ROWS];
        profilingMixedOrderByRows = new Integer[FUNCTION_ROWS];
        for (int index = 0; index < profilingOrderByRows.length; index++) {
            profilingOrderByRows[index] = FUNCTION_ROWS - index - 1;
            profilingAscendingOrderByRows[index] = index;
            profilingMixedOrderByRows[index] = (index * 7919) % FUNCTION_ROWS;
        }
        distinctInput = Flux.range(0, FUNCTION_ROWS).map(index -> index & 1023);
        Integer[] distinctValues = new Integer[FUNCTION_ROWS];
        for (int index = 0; index < distinctValues.length; index++) {
            distinctValues[index] = index & 1023;
        }
        profilingDistinctInput = Flux.fromArray(distinctValues);
        @SuppressWarnings("unchecked")
        Map<String, Object>[] setRows = new Map[2048];
        for (int i = 0; i < setRows.length; i++) {
            setRows[i] = Collections.<String, Object>singletonMap("v", i);
        }
        setLeftSource = Flux.range(0, FUNCTION_ROWS / 2)
                            .map(index -> setRows[index & 1023]);
        setRightSource = Flux.range(0, FUNCTION_ROWS / 2)
                             .map(index -> setRows[512 + (index & 1023)]);
        setSource = name -> "t1".equals(name) ? setLeftSource : setRightSource;
        profilingSetLeftSubscriptions = new AtomicInteger();
        profilingSetRightSubscriptions = new AtomicInteger();
        profilingSetSource = name -> "t1".equals(name)
                ? setLeftSource.doOnSubscribe(ignore -> profilingSetLeftSubscriptions.incrementAndGet())
                : setRightSource.doOnSubscribe(ignore -> profilingSetRightSubscriptions.incrementAndGet());
        Map<String, Object> lookupRow = new HashMap<>();
        lookupRow.put("id", 0);
        lookupRow.put("value", 1);
        Flux<Map<String, Object>> correlatedLookup = Flux.just(lookupRow);
        correlatedSource = name -> "lookup".equals(name) ? correlatedLookup : outerSource;
        publisherInput = outerSource;
        List<Map<String, Object>> ordered = orderBy.start(sortedInput.take(SORT_ROWS))
                                                   .collectList()
                                                   .block();
        if (ordered == null || ordered.size() != SORT_ROWS
                || !Integer.valueOf(FUNCTION_ROWS - SORT_ROWS).equals(ordered.get(0).get("val"))
                || !Integer.valueOf(FUNCTION_ROWS - 1).equals(ordered.get(SORT_ROWS - 1).get("val"))) {
            throw new IllegalStateException("全局排序基准结果不符合预期");
        }
        List<Map<String, Object>> topN = orderByLimit.start(sortedInput).collectList().block();
        if (topN == null || topN.size() != 100
                || !Integer.valueOf(0).equals(topN.get(0).get("val"))
                || !Integer.valueOf(99).equals(topN.get(99).get("val"))) {
            throw new IllegalStateException("Top-N 排序基准结果不符合预期");
        }
        assertTopNOrderByResult(orderByLimit.start(profilingOrderBySource()), "同步 Top-N JFR 输入");
        assertTopNDistribution(profilingAscendingOrderByRows, topN, "升序 Top-N 输入");
        assertTopNDistribution(profilingMixedOrderByRows, topN, "乱序 Top-N 输入");
        singleAsyncOrderKeySubscriptions.set(0);
        assertTopNOrderByResult(singleAsyncOrderByLimit.start(profilingOrderBySource()), "单异步键 Top-N JFR 输入");
        if (singleAsyncOrderKeySubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException("单异步排序键未逐行订阅: "
                                                    + singleAsyncOrderKeySubscriptions.get());
        }
        firstAsyncOrderKeySubscriptions.set(0);
        secondAsyncOrderKeySubscriptions.set(0);
        assertTopNOrderByResult(doubleAsyncOrderByLimit.start(profilingOrderBySource()), "双异步键 Top-N JFR 输入");
        if (firstAsyncOrderKeySubscriptions.get() != FUNCTION_ROWS
                || secondAsyncOrderKeySubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException("双异步排序键未逐行订阅: "
                                                    + firstAsyncOrderKeySubscriptions.get() + "/"
                                                    + secondAsyncOrderKeySubscriptions.get());
        }
        List<Map<String, Object>> distinctOriginal = distinctRows.start(distinctInput).collectList().block();
        List<Map<String, Object>> distinctProfiling = distinctRows.start(profilingDistinctInput).collectList().block();
        if (distinctOriginal == null || distinctProfiling == null
                || distinctOriginal.size() != 1024 || distinctProfiling.size() != 1024
                || !new HashSet<>(distinctOriginal).equals(new HashSet<>(distinctProfiling))) {
            throw new IllegalStateException("DISTINCT profiling 输入与正式输入结果不等价");
        }
        Set<Integer> distinctValuesSeen = new HashSet<>();
        for (Map<String, Object> row : distinctProfiling) {
            if (row.size() != 1 || !(row.get("val") instanceof Integer)) {
                throw new IllegalStateException("DISTINCT profiling 输出字段或类型不正确: " + row);
            }
            distinctValuesSeen.add((Integer) row.get("val"));
        }
        for (int expected = 0; expected < 1024; expected++) {
            if (!distinctValuesSeen.contains(expected)) {
                throw new IllegalStateException("DISTINCT profiling 缺少值: " + expected);
            }
        }
        assertResultCount(intersectRows.start(setSource), 512, "INTERSECT");
        assertProfilingUnionResult(unionRows.start(profilingSetSource), false);
        assertProfilingUnionResult(unionAllRows.start(profilingSetSource), true);
        assertProfilingExceptResult(exceptRows.start(profilingSetSource));
        List<Map<String, Object>> sqlIntersect = intersectRows.start(setSource)
                                                                  .collectList()
                                                                  .block();
        List<Map<String, Object>> nativeIntersect = nativeIntersectRowsResult()
                .collectList()
                .block();
        List<Map<String, Object>> nativeRecordIntersect = nativeRecordIntersectRowsResult()
                .collectList()
                .block();
        if (sqlIntersect == null || nativeIntersect == null
                || nativeRecordIntersect == null
                || sqlIntersect.size() != nativeIntersect.size()
                || sqlIntersect.size() != nativeRecordIntersect.size()
                || !new HashSet<>(sqlIntersect).equals(new HashSet<>(nativeIntersect))
                || !new HashSet<>(sqlIntersect).equals(new HashSet<>(nativeRecordIntersect))) {
            throw new IllegalStateException("INTERSECT 与原生集合结果不一致");
        }
        if (!topN.equals(nativeTopNResult().collectList().block())
                || !topN.equals(nativeTopNResult(profilingOrderBySource()).collectList().block())) {
            throw new IllegalStateException("Top-N 与原生排序结果不一致");
        }
        AtomicInteger leftJoinRightSubscriptions = new AtomicInteger();
        Function<String, Publisher<?>> countedLeftJoinSource = name -> "t2".equals(name)
                ? joinRightSource.doOnSubscribe(ignore -> leftJoinRightSubscriptions.incrementAndGet())
                : joinLeftSource;
        assertLeftJoinResult(leftJoin.start(countedLeftJoinSource), leftJoinRightSubscriptions);
        AtomicInteger rightJoinRightSubscriptions = new AtomicInteger();
        Function<String, Publisher<?>> countedRightJoinSource = name -> "t2".equals(name)
                ? joinRightSource.doOnSubscribe(ignore -> rightJoinRightSubscriptions.incrementAndGet())
                : joinLeftSource;
        assertRightJoinResult(rightJoin.start(countedRightJoinSource), rightJoinRightSubscriptions);
        AtomicInteger correlatedSubscriptions = new AtomicInteger();
        Function<String, Publisher<?>> countedCorrelatedSource = name -> "lookup".equals(name)
                ? Flux.defer(() -> {
                    correlatedSubscriptions.incrementAndGet();
                    return correlatedLookup;
                })
                : outerSource;
        assertResultCount(correlatedSubquery.start(countedCorrelatedSource), FUNCTION_ROWS, "关联子查询");
        if (correlatedSubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException("关联子查询基准未逐行订阅右源: " + correlatedSubscriptions.get());
        }
        assertResultCount(publisherFeature.start(publisherInput), FUNCTION_ROWS, "第三方 Publisher Feature");
        if (!publisherFeature.start(publisherInput).collectList().block()
                .equals(singleArgumentPublisherFunction.start(publisherInput).collectList().block())) {
            throw new IllegalStateException("单参数 Publisher 函数与直接列结果不一致");
        }
        List<Map<String, Object>> paired = twoArgumentPublisherFunction.start(publisherInput)
                                                                       .collectList()
                                                                       .block();
        if (paired == null || paired.size() != FUNCTION_ROWS) {
            throw new IllegalStateException("双参数 Publisher 函数结果行数不一致");
        }
        for (int index = 0; index < paired.size(); index++) {
            if (!Integer.valueOf((index & 1023) * 2).equals(paired.get(index).get("value"))) {
                throw new IllegalStateException("双参数 Publisher 函数结果或顺序不一致: " + index);
            }
        }
        List<Map<String, Object>> multiValueResult = multiValueAggregates.start(multiValueInput)
                                                                         .collectList()
                                                                         .block();
        if (multiValueResult == null || multiValueResult.size() != 1
                || !Long.valueOf(MULTI_VALUE_ROWS).equals(multiValueResult.get(0).get("total"))) {
            throw new IllegalStateException("多值聚合基准结果数量不符合预期");
        }
        Object emitted = multiValueResult.get(0).get("emitted");
        if (!(emitted instanceof CopyOnWriteArrayList)
                || ((List<?>) emitted).size() != MULTI_VALUE_ROWS
                || !Integer.valueOf(0).equals(((List<?>) emitted).get(0))
                || !Integer.valueOf(MULTI_VALUE_ROWS - 1).equals(
                        ((List<?>) emitted).get(MULTI_VALUE_ROWS - 1))) {
            throw new IllegalStateException("多值聚合基准输出类型或顺序不符合预期");
        }
        if (!multiValueResult.equals(nativeMultiValueAggregatesResult().collectList().block())) {
            throw new IllegalStateException("多值聚合与原生收集结果不一致");
        }
        ReactorQLRecord viaView = copyImplicitAliasViaView(joinRows[0]);
        ReactorQLRecord direct = copyImplicitAliasDirectly(joinRows[0]);
        if (!viaView.getRecords(true).equals(direct.getRecords(true))) {
            throw new IllegalStateException("隐式具名来源复制结果不一致");
        }
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long count() {
        return consume(count.start(largeSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long nativeCount() {
        return consume(largeSource.count().flux()
                                  .map(value -> Collections.<String, Object>singletonMap("total", value)));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long globalAggregates() {
        return consume(globalAggregates.start(groupedSource));
    }

    /**
     * JFR-only: avoids the standard consumer's Map.hashCode allocation noise and Flux.range boxing.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingGlobalAggregates(Blackhole blackhole) {
        consumeForProfiling(globalAggregates.start(profilingGroupedSource()), blackhole);
    }

    private Flux<Map<String, Object>> profilingGroupedSource() {
        Flux<Map<String, Object>> source = profilingGroupedSource;
        if (source != null) {
            return source;
        }
        synchronized (this) {
            source = profilingGroupedSource;
            if (source == null) {
                Integer[] indexes = new Integer[LARGE_ROWS];
                for (int index = 0; index < indexes.length; index++) {
                    indexes[index] = index & 1023;
                }
                source = Flux.fromArray(indexes).map(index -> groupedRows[index]);
                Map<String, Object> standardResult = globalAggregates.start(groupedSource).single().block();
                Map<String, Object> profilingResult = globalAggregates.start(source).single().block();
                if (!java.util.Objects.equals(standardResult, profilingResult)) {
                    throw new IllegalStateException("全局聚合 profiling 输入与标准输入结果不一致");
                }
                profilingGroupedSource = source;
            }
            return source;
        }
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long filteredGlobalAggregates() {
        return consume(filteredGlobalAggregates.start(groupedSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long globalAggregatesPublisher() {
        return consume(globalAggregatesPublisher.start(groupedSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long nativeGlobalAggregates() {
        return consume(groupedSource.collect(NativeSummary::new, NativeSummary::add)
                                    .flux()
                                    .map(NativeSummary::toMap));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long nativeRecordGlobalAggregates() {
        return consume(groupedSource
                               .map(row -> ReactorQLRecord.newRecord("test", row, nativeRecordContext))
                               .collect(NativeSummary::new,
                                        (summary, record) -> summary.add((Map<?, ?>) record.getRecord()))
                               .flux()
                               .map(NativeSummary::toMap));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long where() {
        return consume(where.start(largeSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long wherePrebuiltInput() {
        return consume(where.start(profilingWhereInput()));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long nativeWherePrebuiltInput() {
        return consume(nativeWhere(profilingWhereInput()));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long projection() {
        return consume(projection.start(largeSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long wideProjection() {
        return consume(wideProjection.start(wideProjectionInput()));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long starProjection() {
        return consume(starProjection.start(wideProjectionInput()));
    }

    /**
     * JFR-only: consumes result references without traversing the result Map.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingProjection(Blackhole blackhole) {
        consumeForProfiling(projection.start(largeSource), blackhole);
    }

    /**
     * JFR-only: SQL count with two scalar range predicates over prebuilt input.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingWherePrebuiltInput(Blackhole blackhole) {
        consumeForProfiling(where.start(profilingWhereInput()), blackhole);
    }

    /**
     * JFR-only: native Reactor equivalent of the SQL WHERE/count path over the same input.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingNativeWherePrebuiltInput(Blackhole blackhole) {
        consumeForProfiling(nativeWhere(profilingWhereInput()), blackhole);
    }

    /**
     * JFR-only: two-column SQL projection over prebuilt input, without consumer Map hashing.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingProjectionPrebuiltInput(Blackhole blackhole) {
        consumeForProfiling(projection.start(profilingProjectionInput()), blackhole);
    }

    /**
     * JFR-only: native Record equivalent over the same prebuilt input and reference-only consumption.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingNativeRecordProjectionPrebuiltInput(Blackhole blackhole) {
        consumeForProfiling(nativeRecordProjectionRows(profilingProjectionInput()), blackhole);
    }

    /**
     * JFR-only: eight synchronous result columns with prebuilt input and reference-only consumption.
     */
    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public void profilingWideProjection(Blackhole blackhole) {
        consumeForProfiling(wideProjection.start(wideProjectionInput()), blackhole);
    }

    private Flux<Integer> wideProjectionInput() {
        Flux<Integer> source = wideProjectionInput;
        if (source != null) {
            return source;
        }
        synchronized (this) {
            source = wideProjectionInput;
            if (source == null) {
                Integer[] values = new Integer[LARGE_ROWS];
                for (int value = 0; value < values.length; value++) {
                    values[value] = value;
                }
                source = Flux.fromArray(values);
                long[] index = {0};
                wideProjection.start(source).doOnNext(row -> {
                    int value = Math.toIntExact(index[0]++);
                    assertWideProjectionRow(row, value);
                }).then().block();
                if (index[0] != LARGE_ROWS) {
                    throw new IllegalStateException("宽投影基准行数不符合预期: " + index[0]);
                }
                wideProjectionInput = source;
            }
            return source;
        }
    }

    private Flux<Integer> profilingProjectionInput() {
        Flux<Integer> source = wideProjectionInput();
        if (profilingProjectionInputValidated) {
            return source;
        }
        synchronized (this) {
            if (!profilingProjectionInputValidated) {
                projection.start(source)
                          .zipWith(nativeRecordProjectionRows(source), (sql, nativeResult) -> {
                              assertProjectionEquivalent(sql, nativeResult);
                              return 0;
                          })
                          .then()
                          .block();
                profilingProjectionInputValidated = true;
            }
        }
        return source;
    }

    private Flux<Integer> profilingWhereInput() {
        Flux<Integer> source = wideProjectionInput();
        if (profilingWhereInputValidated) {
            return source;
        }
        synchronized (this) {
            if (!profilingWhereInputValidated) {
                AtomicInteger sqlSubscriptions = new AtomicInteger();
                AtomicInteger nativeSubscriptions = new AtomicInteger();
                Map<String, Object> result = Mono.zip(
                                where.start(source.doOnSubscribe(ignore -> sqlSubscriptions.incrementAndGet())).single(),
                                nativeWhere(source.doOnSubscribe(ignore -> nativeSubscriptions.incrementAndGet())).single())
                                                       .map(values -> {
                                                           assertWhereEquivalent(values.getT1(), values.getT2());
                                                           return values.getT1();
                                                       })
                                                       .block();
                if (result == null || sqlSubscriptions.get() != 1 || nativeSubscriptions.get() != 1) {
                    throw new IllegalStateException("WHERE profiling 前置条件的结果或订阅次数不符合预期");
                }
                profilingWhereInputValidated = true;
            }
        }
        return source;
    }

    private static void assertWhereEquivalent(Map<String, Object> sql,
                                              Map<String, Object> nativeResult) {
        if (!sql.equals(nativeResult)) {
            throw new IllegalStateException("WHERE SQL 与原生结果不一致: " + sql + ", " + nativeResult);
        }
        Object sqlValue = sql.get("total");
        Object nativeValue = nativeResult.get("total");
        if (sqlValue == null || nativeValue == null || sqlValue.getClass() != nativeValue.getClass()) {
            throw new IllegalStateException("WHERE SQL 与原生结果类型不一致");
        }
    }

    private static void assertProjectionEquivalent(Map<String, Object> sql,
                                                   Map<String, Object> nativeResult) {
        if (!sql.equals(nativeResult)
                || !new ArrayList<>(sql.keySet()).equals(new ArrayList<>(nativeResult.keySet()))) {
            throw new IllegalStateException("两列表达式投影与原生 Record 结果不一致: " + sql + ", " + nativeResult);
        }
        for (Map.Entry<String, Object> entry : sql.entrySet()) {
            Object nativeValue = nativeResult.get(entry.getKey());
            if (nativeValue == null || nativeValue.getClass() != entry.getValue().getClass()) {
                throw new IllegalStateException("两列表达式投影与原生 Record 类型不一致: " + entry.getKey());
            }
        }
    }

    private static void assertWideProjectionRow(Map<String, Object> actual, int value) {
        Map<String, Object> expected = new HashMap<>(4);
        expected.put("value", value);
        expected.put("plus", value + 1L);
        expected.put("doubled", value * 2L);
        expected.put("difference", value - 3L);
        expected.put("remainder", value % 7L);
        expected.put("half", value / 2L);
        expected.put("non_negative", value >= 0);
        expected.put("first_half", value < 500000);
        // HashMap field iteration is not SQL column ordering; verify every field, value and type below.
        if (!expected.equals(actual)) {
            throw new IllegalStateException("宽投影基准结果不一致: " + actual);
        }
        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            Object actualValue = actual.get(entry.getKey());
            if (actualValue == null || actualValue.getClass() != entry.getValue().getClass()) {
                throw new IllegalStateException("宽投影基准类型不一致: " + entry.getKey());
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long nativeProjection() {
        return consume(largeSource.<Map<String, Object>>handle((value, sink) -> {
            if (value >= 0 && value < LARGE_ROWS) {
                sink.next(projectNative(value));
            }
        }));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long nativeRecordProjection() {
        return consume(nativeRecordProjectionRows(largeSource));
    }

    private static Flux<Map<String, Object>> nativeWhere(Flux<Integer> source) {
        return source.filter(value -> value >= 0 && value < LARGE_ROWS)
                     .count()
                     .map(total -> {
                         Map<String, Object> result = new HashMap<>(4);
                         result.put("total", total);
                         return result;
                     })
                     .flux();
    }

    private Flux<Map<String, Object>> nativeRecordProjectionRows(Flux<Integer> source) {
        return source
                .map(value -> ReactorQLRecord.newRecord("test", value, nativeRecordContext))
                .<Map<String, Object>>handle((record, sink) -> {
                    int value = (Integer) record.getRecord();
                    if (value >= 0 && value < LARGE_ROWS) {
                        sink.next(projectNative(value));
                    }
                });
    }

    private static Map<String, Object> projectNative(int value) {
        Map<String, Object> result = new HashMap<>(4);
        result.put("next_value", CalculateUtils.add(value, 1));
        result.put("doubled", CalculateUtils.multiply(value, 2));
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long commonFunctions() {
        return consume(commonFunctions.start(commonFunctionSource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingCommonFunctions(Blackhole blackhole) {
        consumeForProfiling(commonFunctions.start(commonFunctionSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingRegexpLikeLiteral(Blackhole blackhole) {
        consumeForProfiling(regexpLikeLiteral.start(regexpSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingRegexpLikeDynamic(Blackhole blackhole) {
        consumeForProfiling(regexpLikeDynamic.start(regexpSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingRegexpLikeAlternating(Blackhole blackhole) {
        consumeForProfiling(regexpLikeDynamic.start(regexpAlternatingSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long jsonPath() {
        return consume(jsonPath.start(jsonSource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingJsonGet(Blackhole blackhole) {
        consumeForProfiling(profilingJsonGet.start(profilingJsonGetSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingColdJsonGet(Blackhole blackhole) {
        consumeForProfiling(profilingColdJsonGet.start(profilingJsonGetSource), blackhole);
    }

    /**
     * JFR-only: async projection followed by `t.*`, with prebuilt rows and reference-only consumption.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingAsyncStarProjection(Blackhole blackhole) {
        consumeForProfiling(profilingAsyncStarProjection.start(profilingAsyncStarSource)
                                                               .contextWrite(context -> context.put("profiling-context", "async-star")),
                            blackhole);
    }

    /**
     * JFR-only negative control: same async mapper without star expansion.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingAsyncProjectionWithoutStar(Blackhole blackhole) {
        consumeForProfiling(profilingAsyncProjection.start(profilingAsyncStarSource)
                                                       .contextWrite(context -> context.put("profiling-context", "async-star")),
                            blackhole);
    }

    /**
     * JFR-only negative control: table star expansion without an async mapper.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingSyncTableStarProjection(Blackhole blackhole) {
        consumeForProfiling(profilingSyncTableStarProjection.start(profilingAsyncStarSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long windowAggregates() {
        return consume(windowAggregates.start(groupedSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long windowAggregatesPublisher() {
        return consume(windowAggregatesPublisher.start(groupedSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long collectRows() {
        return consume(collectRows.start(groupedSource));
    }

    @Benchmark
    @OperationsPerInvocation(LARGE_ROWS)
    public long collectRowsPublisher() {
        return consume(collectRowsPublisher.start(groupedSource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long subquery() {
        return consume(subquery.start(subquerySource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long completedReplayRead() {
        return consume(Flux.range(0, FUNCTION_ROWS)
                           .flatMap(ignore -> completedReplayCache));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long completedSnapshotRead() {
        return consume(Flux.range(0, FUNCTION_ROWS)
                           .flatMap(ignore -> Flux.fromIterable(completedCacheRows)));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long completedPublisherRead() {
        return consume(Flux.range(0, FUNCTION_ROWS)
                           .flatMap(ignore -> completedSnapshotPublisher));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long twoAsyncSubqueries() {
        return consume(twoAsyncSubqueries.start(subquerySource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingTwoAsyncSubqueries(Blackhole blackhole) {
        consumeForProfiling(twoAsyncSubqueries.start(subquerySource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long threeAsyncSubqueries() {
        return consume(threeAsyncSubqueries.start(subquerySource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long nestedSubquery() {
        return consume(nestedSubquery.start(subquerySource));
    }

    /**
     * JFR-only: nested subquery with reference-only result consumption.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingNestedSubquery(Blackhole blackhole) {
        consumeForProfiling(nestedSubquery.start(subquerySource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long deeplyNestedSubquery() {
        return consume(deeplyNestedSubquery.start(subquerySource));
    }

    /**
     * JFR-only: deeply nested subquery with reference-only result consumption.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingDeeplyNestedSubquery(Blackhole blackhole) {
        consumeForProfiling(deeplyNestedSubquery.start(subquerySource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long subqueryUncached() {
        return consume(subqueryUncached.start(subquerySource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long existsSubquery() {
        return consume(existsSubquery.start(subquerySource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long existsSubqueryUncached() {
        return consume(existsSubqueryUncached.start(subquerySource));
    }

    @Benchmark
    @OperationsPerInvocation(HIGH_CARDINALITY_ROWS)
    public long highCardinalityAggregates() {
        return consume(highCardinalityAggregates.start(highCardinalitySource));
    }

    /**
     * JFR-only: replays a prebuilt source so per-invocation row construction is not profiled.
     */
    @Benchmark
    @OperationsPerInvocation(HIGH_CARDINALITY_ROWS)
    public void profilingHighCardinalityAggregates(Blackhole blackhole) {
        consumeForProfiling(highCardinalityAggregates.start(profilingHighCardinalitySource()), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(HIGH_CARDINALITY_ROWS)
    public long highCardinalityCount() {
        return consume(highCardinalityCount.start(highCardinalitySource));
    }

    /**
     * JFR-only: replays a prebuilt source so per-invocation row construction is not profiled.
     */
    @Benchmark
    @OperationsPerInvocation(HIGH_CARDINALITY_ROWS)
    public void profilingHighCardinalityCount(Blackhole blackhole) {
        consumeForProfiling(highCardinalityCount.start(profilingHighCardinalitySource()), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(HIGH_CARDINALITY_ROWS)
    public void profilingPerKeyWindowCount(Blackhole blackhole) {
        consumeForProfiling(highCardinalityPerKeyWindowCount.start(profilingHighCardinalitySource()), blackhole);
    }

    private Flux<Map<String, Object>> profilingHighCardinalitySource() {
        Flux<Map<String, Object>> source = profilingHighCardinalitySource;
        if (source != null) {
            return source;
        }
        synchronized (this) {
            source = profilingHighCardinalitySource;
            if (source == null) {
                @SuppressWarnings("unchecked")
                Map<String, Object>[] rows = new Map[HIGH_CARDINALITY_ROWS];
                for (int index = 0; index < rows.length; index++) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("key", "key-" + index);
                    row.put("score", index);
                    row.put("payload", new byte[1024]);
                    rows[index] = row;
                }
                source = Flux.fromArray(rows);
                assertHighCardinalityResult(highCardinalityCount.start(highCardinalitySource),
                                            highCardinalityCount.start(source),
                                            "高基数 count profiling 输入");
                assertHighCardinalityResult(highCardinalityAggregates.start(highCardinalitySource),
                                            highCardinalityAggregates.start(source),
                                            "高基数聚合 profiling 输入");
                Flux<Map<String, Object>> checkedSource = source;
                AtomicInteger subscriptions = new AtomicInteger();
                List<Map<String, Object>> perKey = highCardinalityPerKeyWindowCount
                        .start(Flux.defer(() -> {
                            subscriptions.incrementAndGet();
                            return checkedSource;
                        }))
                        .collectList()
                        .block();
                if (subscriptions.get() != 1 || perKey == null || perKey.size() != HIGH_CARDINALITY_ROWS) {
                    throw new IllegalStateException("按键独立窗口的源订阅或输出行数不符");
                }
                Set<Object> remainingKeys = new HashSet<>();
                for (Map<String, Object> row : rows) {
                    remainingKeys.add(row.get("key"));
                }
                // GROUP BY without ORDER BY has no global key order; verify the complete permutation instead.
                for (Map<String, Object> row : perKey) {
                    if (row.size() != 2
                            || !remainingKeys.remove(row.get("key"))
                            || !java.util.Objects.equals(1L, row.get("total"))) {
                        throw new IllegalStateException("按键独立窗口值、类型或唯一键不符: " + row);
                    }
                }
                if (!remainingKeys.isEmpty()) {
                    throw new IllegalStateException("按键独立窗口存在缺失键: " + remainingKeys.size());
                }
                profilingHighCardinalitySource = source;
            }
            return source;
        }
    }

    private static void assertHighCardinalityResult(Flux<Map<String, Object>> standard,
                                                    Flux<Map<String, Object>> profiling,
                                                    String scenario) {
        List<Map<String, Object>> standardResult = standard.collectList().block();
        List<Map<String, Object>> profilingResult = profiling.collectList().block();
        if (!java.util.Objects.equals(standardResult, profilingResult)) {
            throw new IllegalStateException(scenario + " 与标准输入结果不一致");
        }
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long innerJoin() {
        return consume(innerJoin.start(joinSource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingInnerJoin(Blackhole blackhole) {
        consumeForProfiling(innerJoin.start(joinSource), blackhole);
    }

    /**
     * JFR-only: replays prebuilt 0/1/4/16-match join rows with reference-only result consumption.
     */
    @Benchmark
    @OperationsPerInvocation(JOIN_PROFILE_LEFT_ROWS)
    public void profilingMultiRowInnerJoin(Blackhole blackhole) {
        consumeForProfiling(profilingMultiRowInnerJoin.start(profilingMultiRowJoinSource), blackhole);
    }

    /**
     * JFR-only: same join distribution, with the right ON key supplied by a cold Publisher.
     */
    @Benchmark
    @OperationsPerInvocation(JOIN_PROFILE_LEFT_ROWS)
    public void profilingAsyncOnMultiRowInnerJoin(Blackhole blackhole) {
        consumeForProfiling(profilingAsyncOnMultiRowInnerJoin.start(profilingMultiRowJoinSource), blackhole);
    }

    /**
     * JFR-only: keeps LEFT JOIN fallback signals while avoiding Map.hashCode consumer noise.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingLeftJoin(Blackhole blackhole) {
        consumeForProfiling(leftJoin.start(joinSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(SORT_ROWS)
    public long orderBy() {
        return consume(orderBy.start(sortedInput.take(SORT_ROWS)));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long orderByLimit() {
        return consume(orderByLimit.start(sortedInput));
    }

    /**
     * JFR-only: consumes Top-N result references without traversing result Maps.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingOrderByLimit(Blackhole blackhole) {
        consumeForProfiling(orderByLimit.start(profilingOrderBySource()), blackhole);
    }

    /**
     * Same prebuilt rows and reference-only consumer as profilingOrderByLimit; native Top-N
     * materializes only the 100 emitted result Maps and is an upper bound for this fixed SQL.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingNativeTopN(Blackhole blackhole) {
        consumeForProfiling(nativeTopNResult(profilingOrderBySource()), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingAscendingTopN(Blackhole blackhole) {
        consumeForProfiling(orderByLimit.start(Flux.fromArray(profilingAscendingOrderByRows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingNativeAscendingTopN(Blackhole blackhole) {
        consumeForProfiling(nativeTopNResult(Flux.fromArray(profilingAscendingOrderByRows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingMixedTopN(Blackhole blackhole) {
        consumeForProfiling(orderByLimit.start(Flux.fromArray(profilingMixedOrderByRows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingNativeMixedTopN(Blackhole blackhole) {
        consumeForProfiling(nativeTopNResult(Flux.fromArray(profilingMixedOrderByRows)), blackhole);
    }

    /**
     * JFR-only: isolates one cold asynchronous ORDER BY key without Map traversal noise.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingSingleAsyncOrderByLimit(Blackhole blackhole) {
        consumeForProfiling(singleAsyncOrderByLimit.start(profilingOrderBySource()), blackhole);
    }

    /**
     * JFR-only: isolates two cold asynchronous ORDER BY keys without Map traversal noise.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingDoubleAsyncOrderByLimit(Blackhole blackhole) {
        consumeForProfiling(doubleAsyncOrderByLimit.start(profilingOrderBySource()), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long distinctRows() {
        return consume(distinctRows.start(distinctInput));
    }

    /**
     * JFR-only: consumes DISTINCT result references without traversing result Maps.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingDistinctRows(Blackhole blackhole) {
        consumeForProfiling(distinctRows.start(profilingDistinctInput), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long intersectRows() {
        return consume(intersectRows.start(setSource));
    }

    /**
     * JFR-only: consumes result references without traversing result Maps.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingIntersectRows(Blackhole blackhole) {
        consumeForProfiling(intersectRows.start(setSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long unionRows() {
        return consume(unionRows.start(setSource));
    }

    /**
     * JFR-only: consumes UNION result references without assuming merge order.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingUnionRows(Blackhole blackhole) {
        consumeForProfiling(unionRows.start(setSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long unionAllRows() {
        return consume(unionAllRows.start(setSource));
    }

    /**
     * JFR-only: consumes UNION ALL result references without adding a downstream collection stage.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingUnionAllRows(Blackhole blackhole) {
        consumeForProfiling(unionAllRows.start(setSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long exceptRows() {
        return consume(exceptRows.start(setSource));
    }

    /**
     * JFR-only: consumes the established right-minus-left EXCEPT contract by reference.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingExceptRows(Blackhole blackhole) {
        consumeForProfiling(exceptRows.start(setSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long nativeIntersectRows() {
        return consume(nativeIntersectRowsResult());
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long nativeRecordIntersectRows() {
        return consume(nativeRecordIntersectRowsResult());
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long nativeTopN() {
        return consume(nativeTopNResult());
    }

    private Flux<Map<String, Object>> nativeIntersectRowsResult() {
        return setRightSource
                .collect(HashSet<Map<String, Object>>::new, Set::add)
                .flatMapMany(keys -> setLeftSource
                        .filter(keys::remove)
                        .map(row -> {
                            Map<String, Object> result = new HashMap<>(4);
                            result.put("s.v", row.get("v"));
                            return result;
                        }));
    }

    private Flux<Map<String, Object>> nativeRecordIntersectRowsResult() {
        return setRightSource
                .map(row -> projectSetRecord("t2", row))
                .map(ReactorQLRecord::getRecord)
                .collect(HashSet<Object>::new, Set::add)
                .flatMapMany(keys -> setLeftSource
                        .map(row -> projectSetRecord("t1", row))
                        .filter(record -> keys.remove(record.getRecord()))
                        .map(record -> {
                            Map<String, Object> result = new HashMap<>(4);
                            result.put("s.v", ((Map<?, ?>) record.getRecordValue("s")).get("v"));
                            return result;
                        }));
    }

    private ReactorQLRecord projectSetRecord(String sourceName, Map<String, Object> row) {
        return ReactorQLRecord.newRecord(sourceName, row, nativeRecordContext)
                              .setResult("v", row.get("v"))
                              .resultToRecord("s");
    }

    private Flux<Map<String, Object>> nativeTopNResult() {
        return nativeTopNResult(sortedInput);
    }

    private Flux<Map<String, Object>> nativeTopNResult(Flux<Integer> input) {
        return input
                .collect(() -> new PriorityQueue<Integer>(100, Comparator.reverseOrder()),
                         (queue, value) -> {
                             if (queue.size() < 100) {
                                 queue.offer(value);
                             } else if (value < queue.peek()) {
                                 queue.poll();
                                 queue.offer(value);
                             }
                         })
                .flatMapMany(queue -> {
                    List<Integer> values = new ArrayList<>(queue);
                    Collections.sort(values);
                    return Flux.fromIterable(values);
                })
                .map(value -> {
                    Map<String, Object> result = new HashMap<>(4);
                    result.put("val", value);
                    return result;
                });
    }

    private Flux<Integer> profilingOrderBySource() {
        return Flux.fromArray(profilingOrderByRows);
    }

    private void assertTopNDistribution(Integer[] rows,
                                        List<Map<String, Object>> expected,
                                        String scenario) {
        Flux<Integer> source = Flux.fromArray(rows);
        assertTopNOrderByResult(orderByLimit.start(source), scenario);
        if (!expected.equals(nativeTopNResult(Flux.fromArray(rows)).collectList().block())) {
            throw new IllegalStateException(scenario + " 与原生 Top-N 结果不一致");
        }
    }

    private static ValueMapFeature coldOrderByKey(String name, AtomicInteger subscriptions) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Mono.just(record.getRecord());
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }

    private static ValueMapFeature coldJoinOnKey(String name, AtomicInteger subscriptions) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                net.sf.jsqlparser.expression.Function function =
                        (net.sf.jsqlparser.expression.Function) expression;
                Function<ReactorQLRecord, Publisher<?>> keyMapper = ValueMapFeature.createMapperNow(
                        function.getParameters().getExpressions().get(0), metadata);
                return record -> Mono.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Mono.from(keyMapper.apply(record));
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }

    private static void assertProfilingMultiRowJoinResult(Flux<Map<String, Object>> result, String scenario) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != JOIN_PROFILE_RESULT_ROWS) {
            throw new IllegalStateException(scenario + " 输出行数不符合前置条件");
        }
        int[] matches = new int[4];
        for (Map<String, Object> row : rows) {
            if (row.size() != 2 || !row.containsKey("left_key") || !row.containsKey("right_key")) {
                throw new IllegalStateException(scenario + " 输出字段不符合前置条件: " + row);
            }
            Object left = row.get("left_key");
            Object right = row.get("right_key");
            if (!(left instanceof Integer) || !(right instanceof Integer) || !left.equals(right)) {
                throw new IllegalStateException(scenario + " 输出值或类型不符合前置条件: " + row);
            }
            int key = (Integer) left;
            if (key < 0 || key >= matches.length) {
                throw new IllegalStateException(scenario + " 输出键不符合前置条件: " + key);
            }
            matches[key]++;
        }
        int rowsPerKey = JOIN_PROFILE_LEFT_ROWS / matches.length;
        int[] expected = {0, rowsPerKey, rowsPerKey * 4, rowsPerKey * 16};
        for (int key = 0; key < matches.length; key++) {
            if (matches[key] != expected[key]) {
                throw new IllegalStateException(scenario + " 键 " + key + " 的输出数量不符合前置条件: "
                                                        + matches[key]);
            }
        }
    }

    private static void assertTopNOrderByResult(Flux<Map<String, Object>> result, String scenario) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != 100) {
            throw new IllegalStateException(scenario + " 行数不符合基准前置条件");
        }
        for (int index = 0; index < rows.size(); index++) {
            Map<String, Object> row = rows.get(index);
            if (!Collections.singleton("val").equals(row.keySet())
                    || !(row.get("val") instanceof Integer)
                    || !Integer.valueOf(index).equals(row.get("val"))) {
                throw new IllegalStateException(scenario + " Top-N 顺序、字段或类型不符合基准前置条件: "
                                                        + index + "/" + row);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long leftJoin() {
        return consume(leftJoin.start(joinSource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long rightJoin() {
        return consume(rightJoin.start(joinSource));
    }

    /**
     * JFR-only: keeps RIGHT JOIN's current per-left-row right-source subscription behavior.
     */
    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingRightJoin(Blackhole blackhole) {
        consumeForProfiling(rightJoin.start(joinSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long correlatedSubquery() {
        return consume(correlatedSubquery.start(correlatedSource));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingCorrelatedSubquery(Blackhole blackhole) {
        consumeForProfiling(correlatedSubquery.start(correlatedSource), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long publisherFeature() {
        return consume(publisherFeature.start(publisherInput));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long singleArgumentPublisherFunction() {
        return consume(singleArgumentPublisherFunction.start(publisherInput));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long twoArgumentPublisherFunction() {
        return consume(twoArgumentPublisherFunction.start(publisherInput));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public void profilingTwoArgumentPublisherFunction(Blackhole blackhole) {
        consumeForProfiling(twoArgumentPublisherFunction.start(publisherInput), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(MULTI_VALUE_ROWS)
    public long multiValueAggregates() {
        return consume(multiValueAggregates.start(multiValueInput));
    }

    @Benchmark
    @OperationsPerInvocation(MULTI_VALUE_ROWS)
    public long nativeMultiValueAggregates() {
        return consume(nativeMultiValueAggregatesResult());
    }

    private Flux<Map<String, Object>> nativeMultiValueAggregatesResult() {
        return multiValueInput.collectList().flux().map(values -> {
            Map<String, Object> result = new HashMap<>(4);
            result.put("emitted", new CopyOnWriteArrayList<>(values));
            result.put("total", (long) values.size());
            return result;
        });
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long nativeInnerJoin() {
        return consume(joinLeftSource.flatMap(left -> joinRightSource
                .filter(right -> left.get("v").equals(right.get("v")))
                .map(ignore -> {
                    Map<String, Object> result = new HashMap<>(4);
                    result.put("value", left.get("v"));
                    return result;
                })));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long nativeRecordInnerJoin() {
        return consume(joinLeftSource
                .map(row -> ReactorQLRecord.newRecord("t1", row, nativeRecordContext))
                .flatMap(left -> joinRightSource
                        .map(row -> ReactorQLRecord.newRecord("t2", row, nativeRecordContext)
                                .addRecords(left.getRecords(false)))
                        .filter(record -> {
                            Map<?, ?> leftRow = (Map<?, ?>) record.getRecordValue("t1");
                            Map<?, ?> right = (Map<?, ?>) record.getRecord();
                            return leftRow.get("v").equals(right.get("v"));
                        })
                        .map(record -> {
                            Map<String, Object> result = new HashMap<>(4);
                            result.put("value", ((Map<?, ?>) record.getRecordValue("t1")).get("v"));
                            return result;
                        })));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long namedCopyViaFilteredView() {
        return consume(joinLeftSource
                               .map(this::copyImplicitAliasViaView)
                               .map(ReactorQLBenchmark::projectCopiedValue));
    }

    @Benchmark
    @OperationsPerInvocation(FUNCTION_ROWS)
    public long namedCopyDirectly() {
        return consume(joinLeftSource
                               .map(this::copyImplicitAliasDirectly)
                               .map(ReactorQLBenchmark::projectCopiedValue));
    }

    private ReactorQLRecord copyImplicitAliasViaView(Map<String, Object> leftRow) {
        ReactorQLRecord left = ReactorQLRecord.newRecord("t1", leftRow, nativeRecordContext);
        return ReactorQLRecord.newRecord("t2", joinRightRow, nativeRecordContext)
                              .addRecords(left.getRecords(false));
    }

    private ReactorQLRecord copyImplicitAliasDirectly(Map<String, Object> leftRow) {
        ReactorQLRecord left = ReactorQLRecord.newRecord("t1", leftRow, nativeRecordContext);
        return ReactorQLRecord.newRecord("t2", joinRightRow, nativeRecordContext)
                              .addRecord(left.getName(), left.getRecord());
    }

    private static Map<String, Object> projectCopiedValue(ReactorQLRecord record) {
        Map<String, Object> result = new HashMap<>(4);
        result.put("value", ((Map<?, ?>) record.getRecordValue("t1")).get("v"));
        return result;
    }

    private static long consume(Flux<Map<String, Object>> result) {
        CountingSubscriber subscriber = result.subscribeWith(new CountingSubscriber());
        if (subscriber.error != null) {
            throw new IllegalStateException(subscriber.error);
        }
        if (!subscriber.complete) {
            throw new IllegalStateException("同步基准未在 subscribe 返回前完成");
        }
        return subscriber.count + subscriber.hash;
    }

    private static void consumeForProfiling(Flux<Map<String, Object>> result, Blackhole blackhole) {
        ProfilingSubscriber subscriber = result.subscribeWith(new ProfilingSubscriber(blackhole));
        if (subscriber.error != null) {
            throw new IllegalStateException(subscriber.error);
        }
        if (!subscriber.complete) {
            throw new IllegalStateException("同步基准未在 subscribe 返回前完成");
        }
        blackhole.consume(subscriber.count);
    }

    private static void assertResultCount(Flux<Map<String, Object>> result,
                                          long expected,
                                          String scenario) {
        CountingSubscriber subscriber = result.subscribeWith(new CountingSubscriber());
        if (subscriber.error != null || !subscriber.complete || subscriber.count != expected) {
            throw new IllegalStateException(scenario + " 结果数量或终止信号不符合基准前置条件: "
                                                    + subscriber.count, subscriber.error);
        }
    }

    private static void assertProfilingJsonGetResult(Flux<Map<String, Object>> result) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != FUNCTION_ROWS) {
            throw new IllegalStateException("JSONPath JFR 输入输出行数不符合前置条件");
        }
        for (Map<String, Object> row : rows) {
            if (!Collections.singleton("lon").equals(row.keySet())
                    || !Double.valueOf(120.12).equals(row.get("lon"))) {
                throw new IllegalStateException("JSONPath JFR 输入输出字段、值或类型不符合前置条件: " + row);
            }
        }
    }

    private void assertProfilingAsyncStarResult(Flux<Map<String, Object>> result,
                                                boolean star,
                                                boolean async,
                                                String scenario) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != FUNCTION_ROWS) {
            throw new IllegalStateException(scenario + " 输出行数不符合前置条件");
        }
        List<String> expectedFields = async
                ? (star
                        ? java.util.Arrays.asList("name", "score", "async_id", "id")
                        : java.util.Arrays.asList("name", "score", "id", "async_id"))
                : java.util.Arrays.asList("name", "score", "id");
        for (int index = 0; index < rows.size(); index++) {
            Map<String, Object> row = rows.get(index);
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("id", index);
            expected.put("name", "name-" + (index & 15));
            expected.put("score", index & 255);
            if (async) {
                expected.put("async_id", index + 1L);
            }
            if (!expected.equals(row)
                    || !expectedFields.equals(new ArrayList<>(row.keySet()))) {
                throw new IllegalStateException(scenario + " 输出字段、顺序、类型或值不符合前置条件: " + row);
            }
        }
    }

    private void assertAsyncStarSubscriptions(String scenario) {
        if (profilingAsyncStarSubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException(scenario + " 冷异步列未逐行订阅: "
                                                    + profilingAsyncStarSubscriptions.get());
        }
    }

    private static void assertRegexpLikeResult(Flux<Map<String, Object>> result, String scenario) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != FUNCTION_ROWS) {
            throw new IllegalStateException(scenario + " 输出行数不符合前置条件");
        }
        for (Map<String, Object> row : rows) {
            if (!Collections.singleton("matched").equals(row.keySet())
                    || row.get("matched") != Boolean.TRUE) {
                throw new IllegalStateException(scenario + " 输出字段、类型或值不符合前置条件: " + row);
            }
        }
    }

    private void assertProfilingUnionResult(Flux<Map<String, Object>> result, boolean all) {
        profilingSetLeftSubscriptions.set(0);
        profilingSetRightSubscriptions.set(0);
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != (all ? FUNCTION_ROWS : 1536)) {
            throw new IllegalStateException((all ? "UNION ALL" : "UNION")
                                                    + " 输出行数不符合前置条件");
        }
        int[] counts = new int[1536];
        for (Map<String, Object> row : rows) {
            if (!Collections.singleton("s.v").equals(row.keySet()) || !(row.get("s.v") instanceof Integer)) {
                throw new IllegalStateException((all ? "UNION ALL" : "UNION")
                                                        + " 输出字段或类型不符合前置条件: " + row);
            }
            int value = (Integer) row.get("s.v");
            if (value < 0 || value >= counts.length) {
                throw new IllegalStateException((all ? "UNION ALL" : "UNION")
                                                        + " 输出值不符合前置条件: " + value);
            }
            counts[value]++;
        }
        for (int value = 0; value < counts.length; value++) {
            int expected = all
                    ? sourceRepetitions(value) + sourceRepetitions(value - 512)
                    : 1;
            if (counts[value] != expected) {
                throw new IllegalStateException((all ? "UNION ALL" : "UNION")
                                                        + " 输出重复数不符合前置条件: "
                                                        + value + "/" + counts[value]);
            }
        }
        assertProfilingSetSourceSubscriptions(all ? "UNION ALL" : "UNION");
    }

    private static int sourceRepetitions(int value) {
        if (value < 0 || value >= 1024) {
            return 0;
        }
        return FUNCTION_ROWS / 2 / 1024 + (value < FUNCTION_ROWS / 2 % 1024 ? 1 : 0);
    }

    private void assertProfilingExceptResult(Flux<Map<String, Object>> result) {
        profilingSetLeftSubscriptions.set(0);
        profilingSetRightSubscriptions.set(0);
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != 512) {
            throw new IllegalStateException("EXCEPT (右减左) 输出行数不符合前置条件");
        }
        boolean[] seen = new boolean[512];
        for (Map<String, Object> row : rows) {
            if (!Collections.singleton("s.v").equals(row.keySet()) || !(row.get("s.v") instanceof Integer)) {
                throw new IllegalStateException("EXCEPT (右减左) 输出字段或类型不符合前置条件: " + row);
            }
            int value = (Integer) row.get("s.v");
            int index = value - 1024;
            if (index < 0 || index >= seen.length || seen[index]) {
                throw new IllegalStateException("EXCEPT (右减左) 输出值或重复数不符合前置条件: " + value);
            }
            seen[index] = true;
        }
        for (int index = 0; index < seen.length; index++) {
            if (!seen[index]) {
                throw new IllegalStateException("EXCEPT (右减左) 缺少输出值: " + (index + 1024));
            }
        }
        assertProfilingSetSourceSubscriptions("EXCEPT (右减左)");
    }

    private void assertProfilingSetSourceSubscriptions(String scenario) {
        if (profilingSetLeftSubscriptions.get() != 1 || profilingSetRightSubscriptions.get() != 1) {
            throw new IllegalStateException(scenario + " 输入源订阅次数不符合前置条件: "
                                                    + profilingSetLeftSubscriptions.get() + "/"
                                                    + profilingSetRightSubscriptions.get());
        }
    }

    private static void assertLeftJoinResult(Flux<Map<String, Object>> result,
                                              AtomicInteger rightSubscriptions) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != FUNCTION_ROWS) {
            throw new IllegalStateException("LEFT JOIN 行数不符合基准前置条件");
        }
        int matched = 0;
        int unmatched = 0;
        for (Map<String, Object> row : rows) {
            if (!Collections.singleton("value").equals(row.keySet())
                    || !(row.get("value") instanceof Integer)) {
                throw new IllegalStateException("LEFT JOIN 输出字段或类型不符合基准前置条件: " + row);
            }
            int value = (Integer) row.get("value");
            if (value == 0) {
                matched++;
            } else if (value == 1) {
                unmatched++;
            } else {
                throw new IllegalStateException("LEFT JOIN 输出值不符合基准前置条件: " + row);
            }
        }
        if (matched != FUNCTION_ROWS / 2
                || unmatched != FUNCTION_ROWS / 2
                || rightSubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException("LEFT JOIN 匹配分支、空匹配分支或右源订阅次数不符合基准前置条件: "
                                                    + matched + "/" + unmatched + "/"
                                                    + rightSubscriptions.get());
        }
    }

    private static void assertRightJoinResult(Flux<Map<String, Object>> result,
                                              AtomicInteger rightSubscriptions) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != FUNCTION_ROWS) {
            throw new IllegalStateException("RIGHT JOIN 行数不符合基准前置条件");
        }
        for (Map<String, Object> row : rows) {
            if (!Collections.singleton("value").equals(row.keySet())
                    || !Integer.valueOf(0).equals(row.get("value"))) {
                throw new IllegalStateException("RIGHT JOIN 输出字段、类型或值不符合基准前置条件: " + row);
            }
        }
        if (rightSubscriptions.get() != FUNCTION_ROWS) {
            throw new IllegalStateException("RIGHT JOIN 未逐左行订阅右源: " + rightSubscriptions.get());
        }
    }

    private static void assertNestedSubqueryResult(Flux<Map<String, Object>> result,
                                                    String cachedKey,
                                                    String scenario) {
        List<Map<String, Object>> rows = result.collectList().block();
        if (rows == null || rows.size() != FUNCTION_ROWS) {
            throw new IllegalStateException(scenario + " 行数不符合基准前置条件");
        }
        for (int index = 0; index < rows.size(); index++) {
            Map<String, Object> row = rows.get(index);
            Object cached = row.get("cached");
            if (!java.util.Arrays.asList("o.id", "cached").equals(new ArrayList<>(row.keySet()))
                    || !Integer.valueOf(index & 1023).equals(row.get("o.id"))
                    || !(cached instanceof Map)
                    || !Collections.singleton(cachedKey).equals(((Map<?, ?>) cached).keySet())
                    || !Integer.valueOf(1).equals(((Map<?, ?>) cached).get(cachedKey))) {
                throw new IllegalStateException(scenario + " 值、类型或顺序不符合基准前置条件");
            }
        }
    }

    private static final class NativeSummary {
        private long count;
        private double sum;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;

        private void add(Map<?, ?> row) {
            int score = ((Number) row.get("score")).intValue();
            count++;
            sum += score;
            min = Math.min(min, score);
            max = Math.max(max, score);
        }

        private Map<String, Object> toMap() {
            Map<String, Object> result = new HashMap<>(8);
            result.put("total", count);
            result.put("sum", sum);
            result.put("avg", count == 0 ? 0D : sum / count);
            if (count > 0) {
                result.put("min", min);
                result.put("max", max);
            }
            return result;
        }
    }

    private static final class CountingSubscriber extends BaseSubscriber<Map<String, Object>> {

        private long count;
        private long hash;
        private Throwable error;
        private boolean complete;

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            count++;
            hash += value.hashCode();
        }

        @Override
        protected void hookOnComplete() {
            complete = true;
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
        }
    }

    private static final class ProfilingSubscriber extends BaseSubscriber<Map<String, Object>> {

        private final Blackhole blackhole;
        private long count;
        private Throwable error;
        private boolean complete;

        private ProfilingSubscriber(Blackhole blackhole) {
            this.blackhole = blackhole;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            blackhole.consume(value);
            count++;
        }

        @Override
        protected void hookOnComplete() {
            complete = true;
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
        }
    }
}
