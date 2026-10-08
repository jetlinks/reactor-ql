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

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.*;
import org.jetlinks.reactor.ql.internal.BoundedStateSupport;
import org.jetlinks.reactor.ql.internal.GroupStateBudget;
import org.jetlinks.reactor.ql.internal.SubscriptionContext;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.from.FromTableFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.jetlinks.reactor.ql.utils.SqlUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.function.Consumer3;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.jetlinks.reactor.ql.ReactorQLRecord.newRecord;

/**
 * 默认的ReactorQL实现
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
public class DefaultReactorQL implements ReactorQL {

    public static final String GROUP_NAME_CONTEXT_KEY = "named-group";
    public static final String MULTI_GROUP_CONTEXT_KEY = "multi-group";
    public static final String SETTING_ORDER_BY_MAX_ROWS = "orderBy.maxRows";
    public static final String SETTING_ORDER_BY_WINDOW_SIZE = "orderBy.windowSize";
    public static final String SETTING_ROW_INFO_ENABLED = "rowInfo.enabled";
    public static final String SETTING_JOIN_CONCURRENCY = "join.concurrency";
    public static final String SETTING_GROUP_CONCURRENCY = "group.concurrency";
    public static final String SETTING_GROUP_MAX_ACTIVE_KEYS = "group.maxActiveKeys";
    public static final String SETTING_GROUP_MAX_BUFFERED_ROWS = "group.maxBufferedRows";
    public static final String SETTING_AGGREGATE_FAST_PATH = "aggregate.fastPath";
    public static final String SETTING_SUBQUERY_CACHE = "subquery.cache";
    public static final String SETTING_SUBQUERY_MAX_ROWS = "subquery.maxRows";
    public static final String SETTING_AGGREGATE_MAX_COLLECTION_SIZE = "aggregate.maxCollectionSize";
    public static final String SETTING_DISTINCT_MAX_ROWS = "distinct.maxRows";
    public static final String SETTING_SET_OPERATION_MAX_ROWS = "setOperation.maxRows";

    public static final int DEFAULT_SUBQUERY_MAX_ROWS = Integer.MAX_VALUE;
    public static final int HARD_MAX_SUBQUERY_ROWS = 1_000_000;
    public static final int DEFAULT_AGGREGATE_MAX_COLLECTION_SIZE = Integer.MAX_VALUE;
    public static final int HARD_MAX_AGGREGATE_COLLECTION_SIZE = 1_000_000;
    public static final int DEFAULT_DISTINCT_MAX_ROWS = Integer.MAX_VALUE;
    public static final int HARD_MAX_DISTINCT_ROWS = 1_000_000;
    public static final int DEFAULT_SET_OPERATION_MAX_ROWS = Integer.MAX_VALUE;
    public static final int HARD_MAX_SET_OPERATION_ROWS = 1_000_000;
    public static final String SETTING_SUBQUERY_CACHE_ACTIVE = "_internal.subqueryCacheActive";

    public static final int DEFAULT_GROUP_MAX_ACTIVE_KEYS = Integer.MAX_VALUE;
    public static final int HARD_MAX_GROUP_ACTIVE_KEYS = 1_000_000;
    public static final int DEFAULT_GROUP_MAX_BUFFERED_ROWS = Integer.MAX_VALUE;
    public static final int HARD_MAX_GROUP_BUFFERED_ROWS = 1_000_000;

    private static final int HARD_MAX_ASYNC_CONCURRENCY = 1024;

    private static final Mono<Boolean> alwaysTrue = Mono.just(true);
    private static final Object EMPTY_ASYNC_COLUMN = new Object();

    //行跟踪包装器,用于跟踪行信息
    private static final Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> rowInfoWrapper = flux -> flux
            .elapsed()
            .index((index, row) -> {
                Map<String, Object> rowInfo = new HashMap<>();
                rowInfo.put("index", index + 1); //行号
                rowInfo.put("elapsed", row.getT1()); //自上一行数据已经过去的时间ms
                row.getT2().addRecord("row", rowInfo);
                return row.getT2();
            });


    private final ReactorQLMetadata metadata;

    //select [columnMapper]
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> columnMapper;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> join;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> where;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> groupBy;
    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> orderBy;
    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> limit;
    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> offset;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> distinct;
    private Function<ReactorQLContext, Flux<ReactorQLRecord>> builder;

    private ScalarFilter scalarWhere;
    private RawScalarFilter rawWhere;
    private Function<ReactorQLRecord, ReactorQLRecord> scalarProjection;
    private String executionPlan;


    public DefaultReactorQL(ReactorQLMetadata metadata) {
        this.metadata = metadata;
        prepare();
        metadata.release();
    }


    protected void prepare() {
        where = createWhere();
        columnMapper = createMapper();
        limit = createLimit();
        offset = createOffset();
        groupBy = createGroupBy();
        // Independent reducers, nested groups and result snapshots have observable native error
        // lifecycles. A combined state must not replace those reduce/merge boundaries.
        join = createJoin();
        orderBy = createOrderBy();
        distinct = createDistinct();
        Function<ReactorQLContext, Flux<ReactorQLRecord>> fromMapper = FromFeature
                .createFromMapperByBody(metadata.getSql(), metadata);
        boolean rowInfoEnabled = metadata
                .getSetting(SETTING_ROW_INFO_ENABLED)
                .map(CastUtils::castBoolean)
                .orElse(false);
        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> sourceMapper = rowInfoEnabled
                ? rowInfoWrapper
                : Function.identity();

        PlainSelect select = metadata.getSql();

        String rawWhereSourceName = null;
        String rawWhereSourceAlias = null;
        if (select.getGroupBy() == null
                && rawWhere != null
                && CollectionUtils.isEmpty(select.getJoins())
                && !rowInfoEnabled
                && !metadata.isCheckpoint()
                && select.getFromItem() instanceof Table
                && metadata.getFeatureNow(FeatureId.From.table).getClass() == FromTableFeature.class) {
            Table table = (Table) select.getFromItem();
            String alias = table.getAlias() == null ? table.getName() : table.getAlias().getName();
            if (rawWhere.acceptsSource(SqlUtils.getCleanStr(alias))) {
                rawWhereSourceName = table.getName();
                rawWhereSourceAlias = alias;
            }
        }

        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> wrapper
                = metadata.createWrapper(select);

        SynchronousRowStage synchronousRowStage = metadata.isCheckpoint()
                || scalarWhere == null
                || scalarProjection == null
                ? null
                : new SynchronousRowStage(scalarWhere, scalarProjection);
        executionPlan = describeExecutionPlan(select, synchronousRowStage != null);
        log.debug("ReactorQL execution plan: {}", executionPlan);

        if (null != select.getGroupBy()) {
            Function<ReactorQLContext, Flux<ReactorQLRecord>> aggregateRows = ctx -> groupBy.apply(
                    where.apply(join.apply(sourceMapper.apply(fromMapper.apply(ctx)))));
            builder = ctx ->
                    limit.apply(ctx,
                                offset.apply(ctx,
                                             distinct.apply(
                                                     orderBy.apply(ctx,
                                                             aggregateRows.apply(ctx)
                                                     )
                                             )
                                ))
                         .as(wrapper)
                         .contextWrite(context -> initializeSubscriptionContext(context, ctx));
        } else {
            final String sourceName = rawWhereSourceName;
            final String sourceAlias = rawWhereSourceAlias;
            Function<ReactorQLContext, Flux<ReactorQLRecord>> recordProjectedRows = ctx -> synchronousRowStage == null
                            ? columnMapper.apply(where.apply(join.apply(sourceMapper.apply(fromMapper.apply(ctx)))))
                            : synchronousRowStage.apply(join.apply(sourceMapper.apply(fromMapper.apply(ctx))));
            Function<ReactorQLContext, Flux<ReactorQLRecord>> projectedRows = sourceName == null
                    ? recordProjectedRows
                    // WHERE has the same result-container fallback as aggregate value reads.
                    : ctx -> ctx.getClass() != DefaultReactorQLContext.class
                            ? recordProjectedRows.apply(ctx)
                            : applyRawWhereBeforeRecord(ctx, sourceName, sourceAlias);
            builder = ctx ->
                    limit.apply(ctx,
                                offset.apply(ctx,
                                             distinct.apply(
                                                     orderBy.apply(ctx,
                                                             projectedRows.apply(ctx)
                                                     )
                                             )
                                )
                         )
                         .as(wrapper)
                         .contextWrite(context -> initializeSubscriptionContext(context, ctx));
        }
    }

    private Flux<ReactorQLRecord> applyRawWhereBeforeRecord(ReactorQLContext context,
                                                            String sourceName,
                                                            String sourceAlias) {
        Flux<ReactorQLRecord> rows = context.getDataSource(sourceName).handle((row, sink) -> {
            ReactorQLRecord record;
            if (row instanceof Map) {
                // 默认单表 Map 行没有 Record 可观察副作用；被拒绝行无需创建包装对象。
                if (!rawWhere.testRaw(row)) {
                    return;
                }
                record = newRecord(sourceAlias, row, context);
            } else {
                // 非 Map（包括已有 Record）保留原包装和谓词顺序。
                record = newRecord(sourceAlias, row, context);
                if (!scalarWhere.test(record, record.getRecord())) {
                    return;
                }
            }
            sink.next(scalarProjection == null ? record : scalarProjection.apply(record));
        });
        return scalarProjection == null ? columnMapper.apply(rows) : rows;
    }

    private static Context initializeSubscriptionContext(Context context,
                                                         ReactorQLContext reactorQLContext) {
        Context initialized = context.put(ReactorQLContext.class, reactorQLContext);
        // 嵌套查询沿用根订阅状态，使已证明不相关的多层子查询共享缓存和取消生命周期。
        return initialized.hasKey(SubscriptionContext.class)
                ? initialized
                : initialized.put(SubscriptionContext.class, new SubscriptionContext());
    }

    String describeExecutionPlan() {
        return executionPlan;
    }

    private String describeExecutionPlan(PlainSelect select, boolean compiledRowStage) {
        List<String> stages = new ArrayList<>();
        stages.add("SOURCE");
        if (!CollectionUtils.isEmpty(select.getJoins())) {
            stages.add("ASYNC[join,concurrency="
                               + describeConcurrency(getBoundedConcurrency(SETTING_JOIN_CONCURRENCY)) + "]");
        }
        appendRowStages(stages, select, compiledRowStage);
        if (select.getDistinct() != null) {
            stages.add("STATEFUL[distinct]");
        }
        if (!CollectionUtils.isEmpty(select.getOrderByElements())) {
            stages.add("STATEFUL[order]");
        }
        if (metadata.getSetting(SETTING_SUBQUERY_CACHE_ACTIVE)
                    .map(CastUtils::castBoolean)
                    .orElse(false)) {
            stages.add("OPTIMIZED[subquery-cache,maxRows="
                               + BoundedStateSupport.describeLimit(
                                       metadata.getSetting(SETTING_SUBQUERY_MAX_ROWS)
                                               .map(CastUtils::castNumber)
                                               .map(Number::intValue)
                                               .orElse(DEFAULT_SUBQUERY_MAX_ROWS)
                               )
                               + "]");
        }
        if (metadata.isCheckpoint()) {
            stages.add("DIAGNOSTIC[checkpoint]");
        }
        return String.join(" -> ", stages);
    }

    private void appendRowStages(List<String> stages, PlainSelect select, boolean compiledRowStage) {
        if (select.getGroupBy() != null) {
            if (select.getWhere() != null) {
                stages.add(scalarWhere == null ? "ASYNC[where]" : "SCALAR[where]");
            }
            stages.add("STATEFUL[group,concurrency="
                                   + describeConcurrency(getBoundedConcurrency(SETTING_GROUP_CONCURRENCY))
                                   + ",maxActiveKeys="
                                   + BoundedStateSupport.describeLimit(GroupStateBudget.readMaxActiveKeys(metadata))
                                   + ",maxBufferedRows="
                                   + BoundedStateSupport.describeLimit(GroupStateBudget.readMaxBufferedRows(metadata))
                                   + "]");
            stages.add(scalarProjection == null
                                   ? "ASYNC_OR_STATEFUL[projection]"
                                   : "SCALAR[projection]");
        } else if (compiledRowStage) {
            stages.add("SCALAR[where+projection,handle]");
        } else {
            if (select.getWhere() != null) {
                stages.add(scalarWhere == null ? "ASYNC[where]" : "SCALAR[where]");
            }
            stages.add(scalarProjection == null
                               ? "ASYNC_OR_STATEFUL[projection]"
                               : "SCALAR[projection]");
        }
    }


    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinct() {
        Distinct distinct;
        if ((distinct = metadata.getSql().getDistinct()) == null) {
            return Function.identity();
        }
        return metadata.getFeatureNow(FeatureId.Distinct.of(
                metadata.getSetting("distinctBy").map(String::valueOf).orElse("default")
        )).createDistinctMapper(distinct, metadata);
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createJoin() {
        if (CollectionUtils.isEmpty(metadata.getSql().getJoins())) {
            return Function.identity();
        }
        int concurrency = getBoundedConcurrency(SETTING_JOIN_CONCURRENCY);
        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> mapper = Function.identity();
        //对join的支持
        for (Join joinInfo : metadata.getSql().getJoins()) {

            FromItem from = joinInfo.getRightItem();
            Collection<Expression> on = joinInfo.getOnExpressions();
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter;
            ScalarFilter scalarFilter;
            if (CollectionUtils.isEmpty(on)) {
                //没有条件永远为true
                filter = (ctx, v) -> alwaysTrue;
                scalarFilter = (ctx, value) -> true;
            } else {
                List<BiFunction<ReactorQLRecord, Object, Mono<Boolean>>> filters = new ArrayList<>(on.size());

                for (Expression onExpression : on) {
                    filters.add(FilterFeature.createPredicateNow(onExpression, metadata));
                }
                boolean defaultSingleOn = filters.size() == 1
                        && metadata.getClass() == DefaultReactorQLMetadata.class;
                BiFunction<ReactorQLRecord, Object, Mono<Boolean>> singleFilter = defaultSingleOn
                        ? filters.get(0)
                        : null;
                filter = (reactorQLRecord, o) -> {
                    // Keep metadata flatMap for extensions and explicit concurrency; defer preserves cold ON evaluation.
                    Flux<Boolean> matches = defaultSingleOn && !metadata.getSetting("concurrency").isPresent()
                            ? Flux.defer(() -> singleFilter.apply(reactorQLRecord, o))
                            : metadata.flatMap(Flux.fromIterable(filters),
                                               f -> f.apply(reactorQLRecord, o));
                    return matches.all(Boolean::booleanValue);
                };
                if (!metadata.isCheckpoint() && filters.stream().allMatch(ScalarFilter.class::isInstance)) {
                    scalarFilter = (reactorQLRecord, value) -> {
                        boolean matched = true;
                        for (BiFunction<ReactorQLRecord, Object, Mono<Boolean>> candidate : filters) {
                            matched &= ((ScalarFilter) candidate).test(reactorQLRecord, value);
                        }
                        return matched;
                    };
                } else {
                    scalarFilter = null;
                }
            }

            Function<ReactorQLRecord, Flux<ReactorQLRecord>> rightStreamGetter = null;

            //join (select deviceId,avg(temp) from temp group by interval('10s'),deviceId )
            if (from instanceof SubSelect) {
                String alias = from.getAlias() == null ? null : from.getAlias().getName();
                //子查询
                DefaultReactorQL ql =
                        new DefaultReactorQL(new DefaultReactorQLMetadata(metadata,
                                                                          ((PlainSelect) ((SubSelect) from).getSelectBody())));

                rightStreamGetter = record -> ql
                        .builder
                        .apply(record.bindNamedRecords(record.getContext()
                                                              .transfer((name, flux) -> flux
                                                                      .map(source -> ReactorQLRecord
                                                                              .newRecord(name, source, record.getContext())
                                                                              .addNamedRecords(record)))))
                        // Each derived row owns its aliases/results; rejected JOIN candidates must
                        // not mutate the left row used by outer-join fallback or other outputs.
                        .map(v -> record.copy().addRecord(alias, v.asMap()));

            }
            // join table
            else if ((from instanceof Table)) {
                String name = ((Table) from).getFullyQualifiedName();
                String alias = from.getAlias() == null ? name : from.getAlias().getName();
                rightStreamGetter = left -> left
                        .getDataSource(name)
                        .map(right -> ReactorQLRecord
                                .newRecord(alias, right, left.getContext())
                                .addNamedRecords(left));
            }
            // join unnest(...), explode(...) or other table functions
            else if (from instanceof TableFunction) {
                Function<ReactorQLContext, Flux<ReactorQLRecord>> fromMapper =
                        FromFeature.createFromMapperByFrom(from, metadata);
                rightStreamGetter = left -> fromMapper
                        .apply(left
                                       .getContext()
                                       .transfer((name, flux) -> flux
                                               .map(source -> ReactorQLRecord
                                                       .newRecord(name, source, left.getContext())
                                                       .addNamedRecords(left)))
                                       .bindAll(left.getRecords(true)))
                        .map(right -> right.addNamedRecords(left));
            }
            if (rightStreamGetter == null) {
                throw ReactorQLException.unsupportedFrom(from);
            }
            Function<ReactorQLRecord, Flux<ReactorQLRecord>> fiRightStreamGetter = rightStreamGetter;
            if (joinInfo.isLeft()) {
                mapper = mapper
                        .andThen(flux -> flatMapBounded(
                                flux,
                                left -> filterJoin(fiRightStreamGetter.apply(left), filter, scalarFilter)
                                        .defaultIfEmpty(left),
                                concurrency));

            } else if (joinInfo.isRight()) {
                mapper = mapper
                        .andThen(flux -> flatMapBounded(
                                flux,
                                left -> mapRightJoin(fiRightStreamGetter.apply(left),
                                                     left,
                                                     filter,
                                                     scalarFilter)
                                        .defaultIfEmpty(left),
                                concurrency));
            } else {
                mapper = mapper
                        .andThen(flux -> flatMapBounded(
                                flux,
                                left -> filterJoin(fiRightStreamGetter.apply(left), filter, scalarFilter),
                                concurrency));
            }
        }
        return mapper;
    }

    private static Flux<ReactorQLRecord> filterJoin(
            Flux<ReactorQLRecord> right,
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter,
            ScalarFilter scalarFilter) {
        return scalarFilter == null
                ? right.filterWhen(record -> filter.apply(record, record.getRecord()))
                : right.filter(record -> scalarFilter.test(record, record.getRecord()));
    }

    private static Flux<ReactorQLRecord> mapRightJoin(
            Flux<ReactorQLRecord> right,
            ReactorQLRecord left,
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter,
            ScalarFilter scalarFilter) {
        if (scalarFilter != null) {
            return right.map(record -> scalarFilter.test(record, record.getRecord())
                    ? record
                    : record.removeRecord(left.getName()));
        }
        return right.flatMap(record -> filter
                .apply(record, record.getRecord())
                .map(matched -> matched ? record : record.removeRecord(left.getName())));
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createGroupBy() {
        PlainSelect select = metadata.getSql();
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
            int concurrency = getBoundedConcurrency(SETTING_GROUP_CONCURRENCY);
            AtomicReference<Function<Flux<ReactorQLRecord>, Flux<Tuple2<Flux<ReactorQLRecord>, Map<String, Object>>>>> groupByRef = new AtomicReference<>();

            Consumer3<String, Expression, GroupFeature> featureConsumer = (name, expr, feature) -> {
                //创建分组函数
                Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> mapper = feature.createGroupMapper(expr, metadata);

                //对分组进行命名,比如 group by deviceId ,分组结果应该也能获取到deviceId的值.
                Function<Flux<ReactorQLRecord>, Flux<Tuple2<Flux<ReactorQLRecord>, Map<String, Object>>>> nameMapper =
                        flux -> mapper
                                .apply(flux)
                                .map(group -> {
                                    if (name != null) {
                                        //指定分组命名
                                        return Tuples
                                                .of(group,
                                                    Collections.singletonMap(name, ((GroupedFlux<?, ?>) group).key()));
                                    }
                                    return Tuples.of(group, Collections.emptyMap());
                                });

                //多层分组
                if (groupByRef.get() != null) {
                    groupByRef.set(
                            groupByRef
                                    .get()
                                    .andThen(tp2 -> tp2
                                            .flatMap(parent -> nameMapper
                                                             .apply(parent.getT1())
                                                             .map(child -> {
                                                                 //合并所有分组命名
                                                                 Map<String, Object> zip = new LinkedHashMap<>();
                                                                 zip.putAll(parent.getT2());
                                                                 zip.putAll(child.getT2());
                                                                 return Tuples.of(child.getT1(), zip);
                                                             })
                                                             .contextWrite(ctx -> ctx
                                                                     .put(GROUP_NAME_CONTEXT_KEY, parent.getT2())
                                                                     .put(MULTI_GROUP_CONTEXT_KEY, true)),
                                                     concurrency)
                                    ));
                } else {
                    groupByRef.set(nameMapper);
                }
            };
            for (Expression groupByExpression : groupBy.getGroupByExpressionList().getExpressions()) {
                while (groupByExpression instanceof Parenthesis) {
                    groupByExpression = ((Parenthesis) groupByExpression).getExpression();
                }
                //函数分组, group by interval('1s')
                if (groupByExpression instanceof net.sf.jsqlparser.expression.Function) {
                    featureConsumer.accept(null,
                                           groupByExpression,
                                           metadata.getFeatureNow(
                                                   FeatureId.GroupBy.of(((net.sf.jsqlparser.expression.Function) groupByExpression).getName()),
                                                   groupByExpression::toString));
                }
                //按列分组, group by deviceId
                else if (groupByExpression instanceof Column) {
                    featureConsumer.accept(((Column) groupByExpression).getColumnName(),
                                           groupByExpression,
                                           metadata.getFeatureNow(FeatureId.GroupBy.property));
                }
                //计算分组, group by ts / 1000
                else if (groupByExpression instanceof BinaryExpression) {
                    featureConsumer.accept(null,
                                           groupByExpression,
                                           metadata.getFeatureNow(FeatureId.GroupBy.of(((BinaryExpression) groupByExpression).getStringExpression()),
                                                                  groupByExpression::toString));
                } else {
                    throw ReactorQLException.unsupportedGroupExpression(groupByExpression);
                }
            }

            Function<Flux<ReactorQLRecord>, Flux<Tuple2<Flux<ReactorQLRecord>, Map<String, Object>>>> groupMapper
                    = groupByRef.get();
            if (groupMapper != null) {
                Expression having = select.getHaving();
                //having
                if (null != having) {
                    BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(having, metadata);
                    ScalarFilter scalarFilter = filter instanceof ScalarFilter && !metadata.isCheckpoint()
                            ? (ScalarFilter) filter
                            : null;
                    return flux -> groupMapper
                            .apply(flux)
                            .flatMap(group -> columnMapper
                                             .apply(group.getT1())
                                             //过滤分组结果
                                             .transform(records -> scalarFilter == null
                                                     ? records.filterWhen(ctx -> filter.apply(ctx, ctx.getRecord()))
                                                     : records.filter(ctx -> scalarFilter.test(ctx, ctx.getRecord())))
                                             //分组命名放到上下文里
                                             .contextWrite(Context.of(GROUP_NAME_CONTEXT_KEY, group.getT2())),
                                     concurrency
                            );
                }
                return flux -> groupMapper
                        .apply(flux)
                        .flatMap(group -> columnMapper
                                         .apply(group.getT1())
                                         .contextWrite(Context.of(GROUP_NAME_CONTEXT_KEY, group.getT2())),
                                 concurrency
                        );
            }

        }
        return Function.identity();

    }

    private int getBoundedConcurrency(String setting) {
        int concurrency;
        try {
            Optional<Object> configured = metadata.getSetting(setting);
            if (!configured.isPresent()) {
                return Integer.MAX_VALUE;
            }
            concurrency = CastUtils.castNumber(configured.get()).intValue();
        } catch (RuntimeException error) {
            throw ReactorQLException.invalidArgument(
                    "setting[" + setting + "]必须是数字",
                    "使用 1 到 " + HARD_MAX_ASYNC_CONCURRENCY + " 之间的并发度。",
                    setting + "=32"
            );
        }
        if (concurrency < 1 || concurrency > HARD_MAX_ASYNC_CONCURRENCY) {
            throw ReactorQLException.invalidArgument(
                    "非法并发度 setting[" + setting + "]: " + concurrency,
                    "使用 1 到 " + HARD_MAX_ASYNC_CONCURRENCY + " 之间的并发度，避免无界在途行占用堆内存。",
                    setting + "=32"
            );
        }
        return concurrency;
    }

    private static String describeConcurrency(int concurrency) {
        return concurrency == Integer.MAX_VALUE ? "unbounded" : String.valueOf(concurrency);
    }

    private static <T, R> Flux<R> flatMapBounded(
            Flux<T> source,
            Function<T, ? extends Publisher<? extends R>> mapper,
            int concurrency) {
        return concurrency == 1
                ? source.concatMap(mapper, 0)
                : source.flatMap(mapper, concurrency);
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createWhere() {
        Expression whereExpr = metadata.getSql().getWhere();
        if (whereExpr == null) {
            return Function.identity();
        }
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(whereExpr, metadata);
        if (filter instanceof ScalarFilter) {
            ScalarFilter scalar = (ScalarFilter) filter;
            if (scalar instanceof RawScalarFilter) {
                rawWhere = (RawScalarFilter) scalar;
            }
            ScalarFilter recordFilter = rawWhere == null ? scalar : rawWhere.recordFilter();
            scalarWhere = recordFilter;
            return flux -> flux.filter(ctx -> recordFilter.test(ctx, ctx.getRecord()));
        }
        //where = filterWhen
        return flux -> flux
                .concatMap(ctx -> filter
                        .apply(ctx, ctx.getRecord())
                        .mapNotNull(matched -> matched ? ctx : null));
    }

    protected Optional<Function<ReactorQLRecord, Publisher<?>>> createExpressionMapper(Expression expression) {
        return ValueMapFeature.createMapperByExpression(expression, metadata);
    }

    protected Optional<Function<Flux<ReactorQLRecord>, Flux<Object>>> createAggMapper(Expression expression) {

        AtomicReference<Function<Flux<ReactorQLRecord>, Flux<Object>>> ref = new AtomicReference<>();

        Consumer<ValueAggMapFeature> featureConsumer = feature -> {
            Function<Flux<ReactorQLRecord>, Flux<Object>> mapper = feature.createMapper(expression, metadata);
            ref.set(mapper);
        };
        //仅支持函数聚合: select max(value)
        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            metadata
                    .getFeature(FeatureId.ValueAggMap.of(((net.sf.jsqlparser.expression.Function) expression).getName()))
                    .ifPresent(featureConsumer);
        }
        return Optional.ofNullable(ref.get());

    }

    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper() {

        Map<String, Function<ReactorQLRecord, Publisher<?>>> mappers = new LinkedHashMap<>();

        Map<String, BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> flatMappers = new LinkedHashMap<>();

        Map<String, Function<Flux<ReactorQLRecord>, Flux<Object>>> aggMapper = new LinkedHashMap<>();

        List<Consumer<ReactorQLRecord>> allMapper = new ArrayList<>();

        for (SelectItem selectItem : metadata.getSql().getSelectItems()) {
            selectItem.accept(new SelectItemVisitorAdapter() {
                // select a,b,c
                @Override
                public void visit(SelectExpressionItem item) {
                    Expression expression = item.getExpression();
                    String alias = item.getAlias() == null ? expression.toString() : item.getAlias().getName();
                    String fAlias = SqlUtils.getCleanStr(alias);
                    // select a,b,c
                    createExpressionMapper(expression).ifPresent(mapper -> mappers.put(fAlias, mapper));
                    // select count(),max(val)...
                    createAggMapper(expression).ifPresent(mapper -> aggMapper.put(fAlias, mapper));
                    //flatMap
                    ValueFlatMapFeature.createMapperByExpression(expression, metadata)
                                       .ifPresent(mapper -> flatMappers.put(fAlias, mapper));

                    if (!mappers.containsKey(fAlias) && !aggMapper.containsKey(fAlias) && !flatMappers.containsKey(fAlias)) {
                        throw ReactorQLException.unsupportedExpression(
                                expression,
                                "select 列必须是普通表达式、聚合函数或当前支持的列转行函数；表函数应放到 FROM 子句中使用。",
                                "select count(1) total, date_trunc('minute', timestamp) ts from test group by date_trunc('minute', timestamp)"
                        );
                    }
                }

                //select *
                @Override
                public void visit(AllColumns columns) {
                    allMapper.add(ReactorQLRecord::putRecordToResult);
                }

                //select t.*
                @Override
                public void visit(AllTableColumns columns) {
                    String name;
                    Alias alias = columns.getTable().getAlias();
                    if (alias == null) {
                        name = SqlUtils.getCleanStr(columns.getTable().getName());
                    } else {
                        name = SqlUtils.getCleanStr(alias.getName());
                    }
                    allMapper.add(record -> {
                        Object value = record.getRecordValue(name);
                        if (value instanceof Map) {
                            record.setResults(((Map) value));
                        } else {
                            record.setResult(name, value);
                        }
                    });
                }
            });
        }
        boolean scalarProjection = mappers
                .values()
                .stream()
                .allMatch(ScalarValueMapper.class::isInstance);
        int resultCapacityHint = allMapper.isEmpty() && mappers.size() > 3
                ? mappers.size()
                : 0;
        final Function<ReactorQLRecord, ReactorQLRecord> scalarResultMapper;
        final Function<ReactorQLRecord, Mono<ReactorQLRecord>> resultMapper;
        if (scalarProjection) {
            scalarResultMapper = record -> {
                for (Map.Entry<String, Function<ReactorQLRecord, Publisher<?>>> entry : mappers.entrySet()) {
                    Object value = ((ScalarValueMapper) entry.getValue()).applyScalar(record);
                    if (resultCapacityHint > 0 && record.getClass() == DefaultReactorQLRecord.class) {
                        ((DefaultReactorQLRecord) record).setResult(entry.getKey(), value, resultCapacityHint);
                    } else {
                        record.setResult(entry.getKey(), value);
                    }
                }
                allMapper.forEach(mapper -> mapper.accept(record));
                return record;
            };
            resultMapper = record -> Mono.just(scalarResultMapper.apply(record));
        } else {
            scalarResultMapper = null;
            Function<ReactorQLRecord, Mono<ReactorQLRecord>> asyncResultMapper;
            if (mappers.isEmpty()) {
                asyncResultMapper = Mono::just;
            } else {
                List<ProjectionColumn> scalarColumns = new ArrayList<>();
                List<ProjectionColumn> asyncColumns = new ArrayList<>();
                for (Map.Entry<String, Function<ReactorQLRecord, Publisher<?>>> entry : mappers.entrySet()) {
                    ProjectionColumn column = new ProjectionColumn(entry.getKey(), entry.getValue(), resultCapacityHint);
                    if (entry.getValue() instanceof ScalarValueMapper) {
                        scalarColumns.add(column);
                    } else {
                        asyncColumns.add(column);
                    }
                }
                int asyncSize = asyncColumns.size();
                Function<ReactorQLRecord, Mono<ReactorQLRecord>> asyncColumnsMapper;
                if (asyncSize == 1) {
                    ProjectionColumn column = asyncColumns.get(0);
                    asyncColumnsMapper = record -> Mono
                            .from(column.mapper.apply(record))
                            .map(value -> column.setResult(record, value))
                            // 兼容空异步列：不设置该列，但仍输出当前行。
                            .defaultIfEmpty(record);
                } else {
                    asyncColumnsMapper = record -> {
                        Mono<?>[] sources = new Mono<?>[asyncSize];
                        for (int i = 0; i < asyncSize; i++) {
                            ProjectionColumn column = asyncColumns.get(i);
                            // defer 保持列函数在订阅时调用；空列占位使 zip 不会提前完成或跳过其他列。
                            sources[i] = Mono.<Object>defer(() -> Mono.from(column.mapper.apply(record)))
                                    .defaultIfEmpty(EMPTY_ASYNC_COLUMN);
                        }
                        return Mono.zipDelayError(values -> {
                            for (int i = 0; i < values.length; i++) {
                                if (values[i] != EMPTY_ASYNC_COLUMN) {
                                    asyncColumns.get(i).setResult(record, values[i]);
                                }
                            }
                            return record;
                        }, sources);
                    };
                }
                asyncResultMapper = record -> Mono.defer(() -> {
                    for (ProjectionColumn column : scalarColumns) {
                        column.setResult(record, ((ScalarValueMapper) column.mapper).applyScalar(record));
                    }
                    return asyncColumnsMapper.apply(record);
                });
            }
            if (!allMapper.isEmpty()) {
                Consumer<ReactorQLRecord> allResultMapper = record -> {
                    for (Consumer<ReactorQLRecord> mapper : allMapper) {
                        mapper.accept(record);
                    }
                };
                asyncResultMapper = asyncResultMapper
                        .andThen(record -> record.doOnNext(allResultMapper));
            }
            resultMapper = asyncResultMapper;
        }

        //转换结果集
        boolean hasMapper = !mappers.isEmpty();

        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> mapper;
        //聚合结果
        if (!aggMapper.isEmpty()) {
            int aggSize = aggMapper.size();
            //只有一个聚合时
            if (aggSize == 1) {
                String property = aggMapper.keySet().iterator().next();
                Function<Flux<ReactorQLRecord>, Flux<Object>> oneMapper = aggMapper.values().iterator().next();
                mapper = flux -> {
                    AtomicReference<ReactorQLRecord> cursor = new AtomicReference<>();
                    return metadata.flatMap(
                            flux.doOnNext(cursor::set).as(oneMapper),
                            val -> Mono
                                    .deferContextual(ctx -> {
                                        ReactorQLRecord newCtx = cursor.get();
                                        if (newCtx == null) {
                                            newCtx = ReactorQLRecord
                                                    .newRecord(null,
                                                               new HashMap<>(),
                                                               new DefaultReactorQLContext((r) -> Flux.just(1)));
                                        } else {
                                            newCtx = newCtx.copy();
                                        }
                                        newCtx = newCtx
                                                .putRecordToResult()
                                                .resultToRecord(newCtx.getName())
                                                .setResult(property, val);
                                        //分组名
                                        newCtx.setResults(ctx
                                                                  .<Map<String, Object>>getOrEmpty(GROUP_NAME_CONTEXT_KEY)
                                                                  .orElse(Collections.emptyMap()));

                                        if (hasMapper) {
                                            return resultMapper.apply(newCtx);
                                        }
                                        return Mono.just(newCtx);
                                    })
                    );
                };
            } else {
                mapper = flux -> {

                    AtomicReference<ReactorQLRecord> lastRecordRef = new AtomicReference<>();

                    //多个聚合,将会多次订阅数据流
                    Flux<ReactorQLRecord> temp = flux
                            .doOnNext(lastRecordRef::set)
                            .publish()
                            //全部聚合订阅后才从上游订阅数据
                            .refCount(aggSize);

                    return Flux
                            .merge(
                                    Flux.fromIterable(aggMapper.entrySet())
                                        .map(agg -> agg
                                                .getValue()
                                                .apply(temp)
                                                .map(res -> Tuples.of(agg.getKey(), res))),
                                    aggMapper.size(),
                                    aggMapper.size()
                            )
                            // merge 串行化下游信号，直接累加而不创建逐值 compute 回调。
                            // 保留原 Map 类型：遍历顺序参与 $this 展开后的同名字段覆盖。
                            .<Map<String, Object>>collect(ConcurrentHashMap::new, (map, nameAndValue) -> {
                                String name = nameAndValue.getT1();
                                Object value = nameAndValue.getT2();
                                Object previous = map.get(name);
                                if (previous == null) {
                                    map.put(name, value);
                                } else if (previous instanceof PendingAggregateValues) {
                                    ((PendingAggregateValues) previous).add(value);
                                } else if (previous instanceof List) {
                                    // 保留首值本来是 List 时的原地追加语义。
                                    ((List) previous).add(value);
                                } else {
                                    // 多值在线累加，完成时仍还原原有 COW 结果类型。
                                    map.put(name, new PendingAggregateValues(previous, value));
                                }
                            })
                            .flatMap(map -> Mono
                                    .deferContextual(ctx -> {
                                        map.replaceAll((name, value) -> value instanceof PendingAggregateValues
                                                ? ((PendingAggregateValues) value).finish()
                                                : value);
                                        ReactorQLRecord newCtx = lastRecordRef.get();
                                        //上游没有数据则创建一个新数据
                                        if (newCtx == null) {
                                            newCtx = newRecord(null,
                                                               new ConcurrentHashMap<>(),
                                                               new DefaultReactorQLContext((r) -> Flux.just(1)));
                                        }
                                        //转换上游结果
                                        newCtx = newCtx
                                                .putRecordToResult()
                                                .resultToRecord(newCtx.getName())
                                                .setResults(map);
                                        //添加分组名
                                        newCtx.setResults(ctx.<Map<String, Object>>getOrEmpty(GROUP_NAME_CONTEXT_KEY)
                                                             .orElse(Collections.emptyMap()));
                                        //如果有转换则进行转换
                                        if (hasMapper) {
                                            return resultMapper.apply(newCtx);
                                        }
                                        return Mono.just(newCtx);
                                    }))
                            .flux();
                };
            }
        } else {
            //指定了分组,但是没有聚合.只获取一个结果.
            if (metadata.getSql().getGroupBy() != null) {
                mapper = scalarProjection
                        ? flux -> flux.takeLast(1).map(scalarResultMapper)
                        : flux -> metadata.flatMap(flux.takeLast(1), resultMapper);
            } else if (scalarProjection) {
                mapper = mappers.isEmpty() && allMapper.isEmpty()
                        ? Function.identity()
                        : flux -> flux.map(scalarResultMapper);
            } else {
                mapper = flux -> metadata.flatMap(flux, resultMapper);
            }
        }
        if (flatMappers.isEmpty()) {
            if (aggMapper.isEmpty()
                    && metadata.getSql().getGroupBy() == null
                    && scalarProjection) {
                this.scalarProjection = scalarResultMapper;
            }
            return mapper;
        }

        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> flatMapper = null;
        //组合flatMap函数
        for (Map.Entry<String, BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>>
                flatMapperEntry : flatMappers.entrySet()) {
            String alias = flatMapperEntry.getKey();
            BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> flatMapperFunction = flatMapperEntry.getValue();
            if (flatMapper == null) {
                flatMapper = flux -> flatMapperFunction.apply(alias, flux);
            } else {
                flatMapper = flatMapper.andThen(flux -> flatMapperFunction.apply(alias, flux));
            }
        }
        return flatMapper == null ? mapper :flatMapper.andThen(mapper);
    }

    private static final class ProjectionColumn {

        private final String name;
        private final Function<ReactorQLRecord, Publisher<?>> mapper;
        private final int resultCapacityHint;

        private ProjectionColumn(String name,
                                 Function<ReactorQLRecord, Publisher<?>> mapper,
                                 int resultCapacityHint) {
            this.name = name;
            this.mapper = mapper;
            this.resultCapacityHint = resultCapacityHint;
        }

        private ReactorQLRecord setResult(ReactorQLRecord record, Object value) {
            // 仅内置 Record/Context 首次建容器时使用容量提示；扩展实现保留公开写入契约。
            if (resultCapacityHint > 0 && record.getClass() == DefaultReactorQLRecord.class) {
                return ((DefaultReactorQLRecord) record).setResult(name, value, resultCapacityHint);
            }
            return record.setResult(name, value);
        }
    }

    private static final class PendingAggregateValues {

        private final List<Object> values = new ArrayList<>();

        private PendingAggregateValues(Object first, Object second) {
            values.add(first);
            values.add(second);
        }

        private void add(Object value) {
            values.add(value);
        }

        private List<Object> finish() {
            return new CopyOnWriteArrayList<>(values);
        }
    }

    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createLimit() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null && limit.getRowCount() != null) {
            Expression expr = limit.getRowCount();

            return (ctx, flux) -> {
                Long value = ExpressionUtils
                        .getSimpleValue(expr, ctx)
                        .map(val -> CastUtils.castNumber(val).longValue())
                        .orElse(null);

                if (null == value) {
                    return flux;
                }

                return flux.take(value);
            };
        }
        return (ctx, flux) -> flux;
    }

    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createOffset() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null && limit.getOffset() != null) {
            Expression expr = limit.getOffset();
            return (ctx, flux) -> {
                Long value = ExpressionUtils
                        .getSimpleValue(expr, ctx)
                        .map(val -> CastUtils.castNumber(val).longValue())
                        .orElse(null);

                if (null == value) {
                    return flux;
                }
                return flux.skip(value);
            };
        }
        return (ctx, flux) -> flux;
    }

    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createOrderBy() {
        return OrderBySupport.create(metadata);
    }

    @Override
    public Flux<ReactorQLRecord> start(ReactorQLContext context) {
        return builder
                .apply(context);
    }


    @Override
    public Flux<Map<String, Object>> start(Function<String, Publisher<?>> streamSupplier) {
        return start(new DefaultReactorQLContext(t -> Flux.from(streamSupplier.apply(t))))
                .map(ReactorQLRecord::asMap);
    }

    @Override
    public ReactorQLMetadata metadata() {
        return metadata;
    }
}
