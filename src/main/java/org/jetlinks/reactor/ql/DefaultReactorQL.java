package org.jetlinks.reactor.ql;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.feature.*;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CompareUtils;
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

@Slf4j
public class DefaultReactorQL implements ReactorQL {


    private static final Mono<Boolean> alwaysTrue = Mono.just(true);

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

    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> columnMapper;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> join;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> where;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> groupBy;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> orderBy;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> limit;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> offset;
    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> distinct;
    private Function<ReactorQLContext, Flux<ReactorQLRecord>> builder;


    public DefaultReactorQL(ReactorQLMetadata metadata) {
        this.metadata = metadata;
        prepare();
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
        Function<ReactorQLContext, Flux<ReactorQLRecord>> fromMapper = FromFeature.createFromMapperByBody(metadata.getSql(), metadata);
        PlainSelect select = metadata.getSql();
        if (null != select.getGroupBy()) {
            builder = ctx ->
                    limit.apply(
                            offset.apply(
                                    distinct.apply(
                                            orderBy.apply(
                                                    groupBy.apply(
                                                            where.apply(
                                                                    join.apply(rowInfoWrapper.apply(fromMapper.apply(ctx)))))
                                            )
                                    )
                            )
                    );
        } else {
            builder = ctx ->
                    limit.apply(
                            offset.apply(
                                    distinct.apply(
                                            orderBy.apply(
                                                    columnMapper.apply(
                                                            where.apply(
                                                                    join.apply(rowInfoWrapper.apply(fromMapper.apply(ctx))))
                                                    )
                                            )
                                    )
                            )
                    );
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
        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>
                mapper = Function.identity();
        for (Join joinInfo : metadata.getSql().getJoins()) {
            Expression on = joinInfo.getOnExpression();
            FromItem from = joinInfo.getRightItem();
            BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter;
            if (on == null) {
                filter = (ctx, v) -> alwaysTrue;
            } else {
                filter = FilterFeature.createPredicateNow(on, metadata);
            }

            Function<ReactorQLRecord, Flux<ReactorQLRecord>> rightStreamGetter = null;

            //join (select deviceId,avg(temp) from temp group by interval('10s'),deviceId )
            if (from instanceof SubSelect) {
                String alias = from.getAlias() == null ? null : from.getAlias().getName();
                DefaultReactorQL ql = new DefaultReactorQL(new DefaultReactorQLMetadata(((PlainSelect) ((SubSelect) from)
                        .getSelectBody())));
                rightStreamGetter = record -> ql.builder.apply(
                        record.getContext()
                              .transfer((name, flux) ->
                                                flux.map(source ->
                                                                 newRecord(name, source, record.getContext())
                                                                         .addRecords(record.getRecords(false))))
                              .bindAll(record.getRecords(false))
                ).map(v -> record.addRecord(alias, v.asMap()));

            } else if ((from instanceof Table)) {
                String name = ((Table) from).getFullyQualifiedName();
                String alias = from.getAlias() == null ? name : from.getAlias().getName();
                rightStreamGetter = left -> left.getDataSource(name)
                                                .map(right -> newRecord(alias, right, left.getContext())
                                                        .addRecords(left.getRecords(false)));
            }
            if (rightStreamGetter == null) {
                throw new UnsupportedOperationException("不支持的表关联: " + from);
            }
            Function<ReactorQLRecord, Flux<ReactorQLRecord>> fiRightStreamGetter = rightStreamGetter;
            if (joinInfo.isLeft()) {
                mapper = mapper.andThen(flux ->
                                                flux.flatMap(left -> fiRightStreamGetter
                                                        .apply(left)
                                                        .filterWhen(right -> filter.apply(right, right.getRecord()))
                                                        .defaultIfEmpty(left), Integer.MAX_VALUE));
            } else if (joinInfo.isRight()) {
                mapper = mapper.andThen(flux ->
                                                flux.flatMap(left -> fiRightStreamGetter
                                                        .apply(left)
                                                        .flatMap(right -> filter
                                                                .apply(right, right.getRecord())
                                                                .map(matched -> matched ? right : right.removeRecord(left.getName()))
                                                        )
                                                        .defaultIfEmpty(left), Integer.MAX_VALUE));
            } else {
                mapper = mapper.andThen(flux ->
                                                flux.flatMap(left -> fiRightStreamGetter
                                                        .apply(left)
                                                        .filterWhen(v -> filter.apply(v, v.getRecord())))
                );
            }
        }
        return mapper;
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createGroupBy() {
        PlainSelect select = metadata.getSql();
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
            AtomicReference<Function<Flux<ReactorQLRecord>, Flux<Tuple2< Flux<ReactorQLRecord>, Map<String, Object>>>>> groupByRef = new AtomicReference<>();

            Consumer3<String, Expression, GroupFeature> featureConsumer = (name, expr, feature) -> {

                Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> mapper = feature.createGroupMapper(expr, metadata);

                Function<Flux<ReactorQLRecord>, Flux<Tuple2<Flux<ReactorQLRecord>, Map<String, Object>>>> nameMapper =
                        flux -> mapper.apply(flux)
                                      .map(group -> {
                                          if (name != null) {
                                              //指定分组命名
                                              return Tuples.of(group, Collections.singletonMap(name, ((GroupedFlux<?, ?>) group)
                                                      .key()));
                                          }
                                          return Tuples.of(group, Collections.emptyMap());
                                      });

                if (groupByRef.get() != null) {
                    groupByRef.set(groupByRef.get().andThen(tp2 -> tp2
                            .flatMap(parent -> nameMapper
                                    .apply(parent.getT1())
                                    .map(child -> {
                                        //合并所有分组命名
                                        Map<String, Object> zip = new HashMap<>();
                                        zip.putAll(parent.getT2());
                                        zip.putAll(child.getT2());
                                        return Tuples.of(child.getT1(), zip);
                                    }), Integer.MAX_VALUE)));
                } else {
                    groupByRef.set(nameMapper);
                }
            };
            for (Expression groupByExpression : groupBy.getGroupByExpressions()) {
                if (groupByExpression instanceof net.sf.jsqlparser.expression.Function) {
                    featureConsumer.accept(null, groupByExpression,
                                           metadata.getFeatureNow(
                                                   FeatureId.GroupBy.of(((net.sf.jsqlparser.expression.Function) groupByExpression)
                                                                                .getName())
                                                   , groupByExpression::toString));
                } else if (groupByExpression instanceof Column) {
                    featureConsumer.accept(((Column) groupByExpression).getColumnName(), groupByExpression, metadata.getFeatureNow(FeatureId.GroupBy.property));
                } else if (groupByExpression instanceof BinaryExpression) {
                    featureConsumer.accept(null, groupByExpression,
                                           metadata.getFeatureNow(FeatureId.GroupBy.of(((BinaryExpression) groupByExpression)
                                                                                               .getStringExpression()), groupByExpression::toString));
                } else {
                    throw new UnsupportedOperationException("不支持的分组表达式:" + groupByExpression);
                }
            }

            Function<Flux<ReactorQLRecord>, Flux<Tuple2<Flux<ReactorQLRecord>, Map<String, Object>>>> groupMapper = groupByRef
                    .get();
            if (groupMapper != null) {
                Expression having = select.getHaving();
                if (null != having) {
                    BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(having, metadata);
                    return flux -> groupMapper
                            .apply(flux)
                            .flatMap(group -> columnMapper
                                    .apply(group.getT1())
                                    .filterWhen(ctx -> filter.apply(ctx, ctx.getRecord()))
                                    //分组命名放到上下文里
                                    .subscriberContext(Context.of("named-group", group.getT2()))
                            );
                }
                return flux -> groupMapper.apply(flux)
                                          .flatMap(group -> columnMapper.apply(group.getT1())
                                                                        .subscriberContext(Context.of("named-group", group
                                                                                .getT2()))
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
        return flux -> flux.filterWhen(ctx -> filter.apply(ctx, ctx.getRecord()));
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
                @Override
                public void visit(SelectExpressionItem item) {
                    Expression expression = item.getExpression();
                    String alias = item.getAlias() == null ? expression.toString() : item.getAlias().getName();
                    if (alias.startsWith("\"")) {
                        alias = alias.substring(1);
                    }
                    if (alias.endsWith("\"")) {
                        alias = alias.substring(0, alias.length() - 1);
                    }
                    String fAlias = alias;
                    createExpressionMapper(expression).ifPresent(mapper -> mappers.put(fAlias, mapper));
                    createAggMapper(expression).ifPresent(mapper -> aggMapper.put(fAlias, mapper));
                    //flatMap
                    ValueFlatMapFeature.createMapperByExpression(expression, metadata)
                                       .ifPresent(mapper -> flatMappers.put(fAlias, mapper));

                    if (!mappers.containsKey(alias) && !aggMapper.containsKey(alias) && !flatMappers.containsKey(alias)) {
                        throw new UnsupportedOperationException("不支持的操作:" + expression);
                    }
                }

                @Override
                public void visit(AllColumns columns) {
                    allMapper.add(ReactorQLRecord::putRecordToResult);
                }

                @Override
                public void visit(AllTableColumns columns) {
                    String name;
                    Alias alias = columns.getTable().getAlias();
                    if (alias == null) {
                        name = columns.getTable().getName();
                    } else {
                        name = alias.getName();
                    }
                    allMapper.add(record -> record.getRecord(name).ifPresent(v -> {
                        if (v instanceof Map) {
                            record.setResults(((Map) v));
                        } else {
                            record.setResult(name, v);
                        }
                    }));
                }
            });
        }
        Function<ReactorQLRecord, Mono<ReactorQLRecord>> _resultMapper = record ->
                Flux.fromIterable(mappers.entrySet())
                    .flatMap(e -> Mono.zip(Mono.just(e.getKey()), Mono.from(e.getValue().apply(record))))
                    .doOnNext(tp2 -> record.setResult(tp2.getT1(), tp2.getT2()))
                    .then()
                    .thenReturn(record);

        if (!allMapper.isEmpty()) {
            _resultMapper = _resultMapper
                    .andThen(record -> record.doOnNext(r -> {
                        allMapper.forEach(mapper -> mapper.accept(r));
                    }));
        }

        //转换结果集
        Function<ReactorQLRecord, Mono<ReactorQLRecord>> resultMapper = _resultMapper;
        boolean hasMapper = !mappers.isEmpty();

        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> mapper;
        //聚合结果
        if (!aggMapper.isEmpty()) {
            int aggSize = aggMapper.size();
            if (aggSize == 1) {
                String property = aggMapper.keySet().iterator().next();
                Function<Flux<ReactorQLRecord>, Flux<Object>> oneMapper = aggMapper.values().iterator().next();
                mapper = flux -> {
                    AtomicReference<ReactorQLRecord> cursor = new AtomicReference<>();
                    return flux
                            .doOnNext(cursor::set)
                            .as(oneMapper)
                            .flatMap(val -> Mono
                                    .subscriberContext()
                                    .flatMap(ctx -> {
                                        ReactorQLRecord newCtx = cursor.get();
                                        if (newCtx == null) {
                                            newCtx = newRecord(null, new HashMap<>(), new DefaultReactorQLContext((r) -> Flux
                                                    .just(1)));
                                        } else {
                                            newCtx = newCtx.copy();
                                        }
                                        newCtx = newCtx
                                                .putRecordToResult()
                                                .resultToRecord(newCtx.getName())
                                                .setResult(property, val);

                                        newCtx.setResults(ctx.<Map<String, Object>>getOrEmpty("named-group").orElse(Collections
                                                                                                                            .emptyMap()));

                                        if (hasMapper) {
                                            return resultMapper.apply(newCtx);
                                        }
                                        return Mono.just(newCtx);
                                    }));
                };
            } else {
                mapper = flux -> {

                    AtomicReference<ReactorQLRecord> cursor = new AtomicReference<>();

                    Flux<ReactorQLRecord> temp = flux
                            .doOnNext(cursor::set)
                            .publish()
                            .refCount(aggSize);

                    return Flux
                            .fromIterable(aggMapper.entrySet())
                            .map(agg -> agg.getValue()
                                           .apply(temp)
                                           .map(res -> Tuples.of(agg.getKey(), res)))
                            .as(Flux::merge)
                            // TODO: 2020/8/21 更好的多列聚合处理方式?
                            .<Map<String, Object>>collect(ConcurrentHashMap::new, (map, v) -> {
                                map.compute(v.getT1(), (v1, v2) -> {
                                    if (v2 != null) {
                                        if (v2 instanceof List) {
                                            ((List) v2).add(v.getT2());
                                            return v2;
                                        } else {
                                            List<Object> values = new CopyOnWriteArrayList<>();
                                            values.add(v2);
                                            values.add(v.getT2());
                                            return values;
                                        }
                                    }
                                    return v.getT2();
                                });
                            })
                            .flatMap(map -> {
                                return Mono.subscriberContext()
                                           .flatMap(ctx -> {


                                               ReactorQLRecord newCtx = cursor.get();
                                               if (newCtx == null) {
                                                   newCtx = newRecord(null, new HashMap<>(), new DefaultReactorQLContext((r) -> Flux
                                                           .just(1)));
                                               }
                                               newCtx = newCtx
                                                       .putRecordToResult()
                                                       .resultToRecord(newCtx.getName())
                                                       .setResults(map);
                                               newCtx.setResults(ctx.<Map<String, Object>>getOrEmpty("named-group").orElse(Collections
                                                                                                                                   .emptyMap()));
                                               if (hasMapper) {
                                                   return resultMapper.apply(newCtx);
                                               }
                                               return Mono.just(newCtx);
                                           });
                            })
                            .flux();
                };
            }
        } else {
            //指定了分组,但是没有聚合.只获取一个结果.
            if (metadata.getSql().getGroupBy() != null) {
                mapper = flux -> flux.takeLast(1).flatMap(resultMapper);
            } else {
                mapper = flux -> flux.flatMap(resultMapper);
            }
        }
        if (flatMappers.isEmpty()) {
            return mapper;
        }

        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> flatMapper = null;
        for (Map.Entry<String, BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> flatMapperEntry
                : flatMappers.entrySet()) {
            String alias = flatMapperEntry.getKey();
            if (flatMapper == null) {
                flatMapper = flux -> flatMapperEntry.getValue().apply(alias, flux);
            } else {
                flatMapper = flatMapper.andThen(flux -> flatMapperEntry.getValue().apply(alias, flux));
            }
        }
        return flatMapper;
    }

    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createLimit() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getRowCount();
            if (expr instanceof LongValue) {
                return flux -> flux.take(((LongValue) expr).getValue());
            }
        }
        return Function.identity();
    }

    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createOffset() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getOffset();
            if (expr instanceof LongValue) {
                return flux -> flux.skip(((LongValue) expr).getValue());
            }
        }
        return Function.identity();
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
