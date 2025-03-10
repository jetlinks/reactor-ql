package org.jetlinks.reactor.ql;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.feature.*;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
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

    private static final Mono<Boolean> alwaysTrue = Mono.just(true);

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
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> orderBy;
    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> limit;
    private BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> offset;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> distinct;
    private Function<ReactorQLContext, Flux<ReactorQLRecord>> builder;


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
        join = createJoin();
        orderBy = createOrderBy();
        distinct = createDistinct();
        Function<ReactorQLContext, Flux<ReactorQLRecord>> fromMapper = FromFeature
                .createFromMapperByBody(metadata.getSql(), metadata);

        PlainSelect select = metadata.getSql();

        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> wrapper
                = metadata.createWrapper(select);

        if (null != select.getGroupBy()) {
            builder = ctx ->
                    limit.apply(ctx,
                                offset.apply(ctx,
                                             distinct.apply(
                                                     orderBy.apply(
                                                             groupBy.apply(
                                                                     where.apply(
                                                                             join.apply(rowInfoWrapper.apply(fromMapper.apply(ctx)))))
                                                     )
                                             )
                                ))
                         .as(wrapper)
                         .contextWrite(context -> context.put(ReactorQLContext.class, ctx));
        } else {
            builder = ctx ->
                    limit.apply(ctx,
                                offset.apply(ctx,
                                             distinct.apply(
                                                     orderBy.apply(
                                                             columnMapper.apply(
                                                                     where.apply(
                                                                             join.apply(rowInfoWrapper.apply(fromMapper.apply(ctx))))
                                                             )
                                                     )
                                             )
                                )
                         )
                         .as(wrapper)
                         .contextWrite(context -> context.put(ReactorQLContext.class, ctx));
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
        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> mapper = Function.identity();
        //对join的支持
        for (Join joinInfo : metadata.getSql().getJoins()) {

            FromItem from = joinInfo.getRightItem();
            Collection<Expression> on = joinInfo.getOnExpressions();
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter;
            if (CollectionUtils.isEmpty(on)) {
                //没有条件永远为true
                filter = (ctx, v) -> alwaysTrue;
            } else {
                List<BiFunction<ReactorQLRecord, Object, Mono<Boolean>>> filters = new ArrayList<>(on.size());

                for (Expression onExpression : on) {
                    filters.add(FilterFeature.createPredicateNow(onExpression, metadata));
                }
                filter = (reactorQLRecord, o) -> metadata
                        .flatMap(Flux.fromIterable(filters),
                                 f -> f.apply(reactorQLRecord, o))
                        .all(Boolean::booleanValue);
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
                        .apply(record.getContext()
                                     .transfer((name, flux) -> flux
                                             .map(source -> ReactorQLRecord
                                                     .newRecord(name, source, record.getContext())
                                                     .addRecords(record.getRecords(false))))
                                     //把行结果绑定到参数中,可以在子查询SQL中使用.
                                     .bindAll(record.getRecords(false)))
                        //添加记录到原始查询结果中
                        .map(v -> record.addRecord(alias, v.asMap()));

            }
            // join table
            else if ((from instanceof Table)) {
                String name = ((Table) from).getFullyQualifiedName();
                String alias = from.getAlias() == null ? name : from.getAlias().getName();
                rightStreamGetter = left -> left
                        .getDataSource(name)
                        .map(right -> ReactorQLRecord
                                .newRecord(alias, right, left.getContext())
                                .addRecords(left.getRecords(false)));
            }
            if (rightStreamGetter == null) {
                throw new UnsupportedOperationException("不支持的表关联: " + from);
            }
            Function<ReactorQLRecord, Flux<ReactorQLRecord>> fiRightStreamGetter = rightStreamGetter;
            if (joinInfo.isLeft()) {
                mapper = mapper
                        .andThen(flux -> flux
                                .flatMap(left -> fiRightStreamGetter
                                                 .apply(left)
                                                 .filterWhen(right -> filter.apply(right, right.getRecord()))
                                                 .defaultIfEmpty(left),
                                         Integer.MAX_VALUE));

            } else if (joinInfo.isRight()) {
                mapper = mapper
                        .andThen(flux -> flux
                                .flatMap(left -> fiRightStreamGetter
                                                 .apply(left)
                                                 .flatMap(right -> filter
                                                         .apply(right, right.getRecord())
                                                         //没有匹配上,则移除结果
                                                         .map(matched -> matched ? right : right.removeRecord(left.getName()))
                                                 )
                                                 .defaultIfEmpty(left),
                                         Integer.MAX_VALUE));
            } else {
                mapper = mapper
                        .andThen(flux -> flux
                                .flatMap(left -> fiRightStreamGetter
                                                 .apply(left)
                                                 .filterWhen(v -> filter.apply(v, v.getRecord())),
                                         Integer.MAX_VALUE)
                        );
            }
        }
        return mapper;
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createGroupBy() {
        PlainSelect select = metadata.getSql();
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
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
                                                     Integer.MAX_VALUE)
                                    ));
                } else {
                    groupByRef.set(nameMapper);
                }
            };
            for (Expression groupByExpression : groupBy.getGroupByExpressionList().getExpressions()) {
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
                    throw new UnsupportedOperationException("Unsupported group expression:" + groupByExpression);
                }
            }

            Function<Flux<ReactorQLRecord>, Flux<Tuple2<Flux<ReactorQLRecord>, Map<String, Object>>>> groupMapper
                    = groupByRef.get();
            if (groupMapper != null) {
                Expression having = select.getHaving();
                //having
                if (null != having) {
                    BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(having, metadata);
                    return flux -> groupMapper
                            .apply(flux)
                            .flatMap(group -> columnMapper
                                             .apply(group.getT1())
                                             //过滤分组结果
                                             .filterWhen(ctx -> filter.apply(ctx, ctx.getRecord()))
                                             //分组命名放到上下文里
                                             .contextWrite(Context.of(GROUP_NAME_CONTEXT_KEY, group.getT2())),
                                     Integer.MAX_VALUE
                            );
                }
                return flux -> groupMapper
                        .apply(flux)
                        .flatMap(group -> columnMapper
                                         .apply(group.getT1())
                                         .contextWrite(Context.of(GROUP_NAME_CONTEXT_KEY, group.getT2())),
                                 Integer.MAX_VALUE
                        );
            }

        }
        return Function.identity();

    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createWhere() {
        Expression whereExpr = metadata.getSql().getWhere();
        if (whereExpr == null) {
            return Function.identity();
        }
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(whereExpr, metadata);
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
                        throw new UnsupportedOperationException("Unsupported expression:" + expression);
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
                    allMapper.add(record -> record
                            .getRecord(name)
                            .ifPresent(v -> {
                                if (v instanceof Map) {
                                    record.setResults(((Map) v));
                                } else {
                                    record.setResult(name, v);
                                }
                            }));
                }
            });
        }
        Function<ReactorQLRecord, Mono<ReactorQLRecord>> _resultMapper;

        if (mappers.isEmpty()) {
            _resultMapper = Mono::just;
        } else {
            int size = mappers.size();
            _resultMapper = record ->
                    Flux.fromIterable(mappers.entrySet())
                        .flatMapDelayError(e -> Mono
                                                   .from(e.getValue().apply(record))
                                                   .doOnNext(val -> record.setResult(e.getKey(), val)),
                                           size,
                                           size)
                        .then()
                        .thenReturn(record);
        }

        if (!allMapper.isEmpty()) {
            _resultMapper = _resultMapper
                    .andThen(record -> record.doOnNext(r -> allMapper.forEach(mapper -> mapper.accept(r))));
        }

        //转换结果集
        Function<ReactorQLRecord, Mono<ReactorQLRecord>> resultMapper = _resultMapper;
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
                            //把全部聚合收集到map里
                            // TODO: 2020/8/21 还有更好的多列聚合处理方式?
                            .<Map<String, Object>>collect(ConcurrentHashMap::new, (map, nameAndValue) -> {
                                String name = nameAndValue.getT1();
                                Object value = nameAndValue.getT2();
                                map.compute(name, (key, _value) -> {
                                    //已经存在值,可能聚合函数返回的是多个结果
                                    if (_value != null) {
                                        //替换值为List

                                        if (_value instanceof List) {
                                            ((List) _value).add(value);
                                            return _value;
                                        } else {
                                            List<Object> values = new CopyOnWriteArrayList<>();
                                            values.add(_value);
                                            values.add(value);
                                            return values;
                                        }
                                    }
                                    return value;
                                });
                            })
                            .flatMap(map -> Mono
                                    .deferContextual(ctx -> {
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
                mapper = flux -> metadata.flatMap(flux.takeLast(1), resultMapper);
            } else {
                mapper = flux -> metadata.flatMap(flux, resultMapper);
            }
        }
        if (flatMappers.isEmpty()) {
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
        return flatMapper;
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

    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createOrderBy() {

        List<OrderByElement> orders = metadata.getSql().getOrderByElements();
        if (CollectionUtils.isEmpty(orders)) {
            return Function.identity();
        }
        Comparator<ReactorQLRecord> comparator = null;
        for (OrderByElement order : orders) {
            Expression expr = order.getExpression();
            Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expr, metadata);

            Comparator<ReactorQLRecord> exprComparator = (left, right) ->
                    Mono.zip(
                            Mono.from(mapper.apply(left)),
                            Mono.from(mapper.apply(right)),
                            CompareUtils::compare
                    ).toFuture().getNow(-1); // TODO: 2020/4/2 不支持异步的order函数

            if (!order.isAsc()) {
                exprComparator = exprComparator.reversed();
            }
            if (comparator == null) {
                comparator = exprComparator;
            } else {
                comparator = comparator.thenComparing(exprComparator);
            }
        }
        Comparator<ReactorQLRecord> fiComparator = comparator;
        return flux -> flux.sort(fiComparator);

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
