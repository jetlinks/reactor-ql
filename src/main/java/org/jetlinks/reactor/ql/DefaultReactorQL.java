package org.jetlinks.reactor.ql;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.feature.*;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.function.Function;

@Slf4j
public class DefaultReactorQL implements ReactorQL {


    public DefaultReactorQL(ReactorQLMetadata metadata) {
        this.metadata = metadata;
        prepare();
    }

    private ReactorQLMetadata metadata;

    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> where;
    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> columnMapper;
    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> limit;
    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> offset;
    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> groupBy;

    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> join;


    Function<ReactorQLContext, Flux<ReactorQLRecord>> builder;

    private ReactorQLRecord newContext(String name, Object row, ReactorQLContext context) {
        if (row instanceof ReactorQLRecord) {
            return ((ReactorQLRecord) row);//.addRecord(name,((ReactorQLContext) row).getRecord());
        }
        return new DefaultReactorQLRecord(name, row, context);
    }

    private Flux<ReactorQLRecord> mapContext(String name, ReactorQLContext context, Flux<Object> flux) {
        return flux.map(obj -> newContext(name, obj, context));
    }

    protected void prepare() {
        where = createWhere();
        columnMapper = createMapper();
        limit = createLimit();
        offset = createOffset();
        groupBy = createGroupBy();
        join = createJoin();

        Function<ReactorQLContext, Flux<ReactorQLRecord>> fromMapper = FromFeature.createFromMapperByBody(metadata.getSql(), metadata);

        PlainSelect select = metadata.getSql();
        FromItem from = select.getFromItem();
        {
            if (null != select.getGroupBy()) {
                builder = ctx ->
                        limit.apply(offset.apply(
                                groupBy.apply(
                                        where.apply(
                                                join.apply(fromMapper.apply(ctx))))
                                )
                        );
            } else {
                builder = ctx ->
                        limit.apply(
                                offset.apply(
                                        columnMapper.apply(
                                                where.apply(
                                                        join.apply(fromMapper.apply(ctx)))
                                        )
                                )
                        );
            }
        }

//        if (from == null) {
//
//        } else if (from instanceof Table) {
//            Table table = (Table) from;
//            String tableName = table.getName();
//            String alias = table.getAlias() != null ? table.getAlias().getName() : tableName;
//            if (null != select.getGroupBy()) {
//                builder = supplier -> limit.apply(offset.apply(groupBy.apply(where.apply(join.apply(supplier.getDataSource(tableName).as(flx -> mapContext(alias, supplier, flx)))))));
//            } else {
//                builder = supplier -> limit.apply(offset.apply(columnMapper.apply(where.apply(join.apply(supplier.getDataSource(tableName).as(flx -> mapContext(alias, supplier, flx)))))));
//            }
//        } else {
//            SelectBody body = null;
//            if (from instanceof SubSelect) {
//                body = (((SubSelect) from).getSelectBody());
//            }
//            if (body instanceof PlainSelect) {
//                String name = from.getAlias() != null ? from.getAlias().getName() : null;
//                PlainSelect plainSelect = ((PlainSelect) body);
//                DefaultReactorQL child = new DefaultReactorQL(new DefaultReactorQLMetadata(plainSelect));
//                if (null != select.getGroupBy()) {
//                    builder = ctx -> limit.apply(offset.apply(groupBy.apply(where.apply(join.apply(child.builder.apply(ctx).map(v -> v.resultToRecord(name)))))));
//                } else {
//                    builder = ctx -> limit.apply(offset.apply(columnMapper.apply(where.apply(join.apply(child.builder.apply(ctx).map(v -> v.resultToRecord(name)))))));
//                }
//            }
//        }
        if (builder == null) {
            throw new UnsupportedOperationException("不支持的SQL语句:" + select);
        }
    }

    static Mono<Boolean> alwaysTrue = Mono.just(true);


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
                DefaultReactorQL ql = new DefaultReactorQL(new DefaultReactorQLMetadata(((PlainSelect) ((SubSelect) from).getSelectBody())));

                rightStreamGetter = record -> ql.builder.apply(
                        record.getContext()
                                .wrap((name, flux) -> {
                                    return flux
                                            .map(source -> newContext(name, source, record.getContext())
                                                    .addRecord(record.getName(), record.getRecord()));
                                })
                ).map(v -> record.addRecord(alias, v.asMap()));


            } else if ((from instanceof Table)) {
                String name = ((Table) from).getFullyQualifiedName();
                String alias = from.getAlias() == null ? name : from.getAlias().getName();
                rightStreamGetter = ctx -> ctx.getDataSource(name)
                        .map(right -> newContext(alias, right, ctx.getContext()).addRecord(ctx.getName(), ctx.getRecord()));
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
                                .defaultIfEmpty(left)));
            } else if (joinInfo.isRight()) {
                mapper = mapper.andThen(flux ->
                        flux.flatMap(left -> fiRightStreamGetter
                                .apply(left)
                                .flatMap(right -> filter
                                        .apply(right, right.getRecord())
                                        .map(matched -> matched ? right : right.removeRecord(left.getName()))
                                )
                                .defaultIfEmpty(left)));
            } else {
                mapper = mapper.andThen(flux ->
                        flux.flatMap(left -> fiRightStreamGetter.apply(left).filterWhen(v -> filter.apply(v, v.getRecord())))
                );
            }
        }
        return mapper;
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createGroupBy() {
        PlainSelect select = metadata.getSql();
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
            AtomicReference<Function<Flux<ReactorQLRecord>, Flux<? extends Flux<ReactorQLRecord>>>> groupByRef = new AtomicReference<>();
            BiConsumer<Expression, GroupFeature> featureConsumer = (expr, feature) -> {
                Function<Flux<ReactorQLRecord>, Flux<? extends Flux<ReactorQLRecord>>> mapper = feature.createGroupMapper(expr, metadata);
                if (groupByRef.get() != null) {
                    groupByRef.set(groupByRef.get().andThen(flux -> flux.flatMap(mapper)));
                } else {
                    groupByRef.set(mapper);
                }
            };
            for (Expression groupByExpression : groupBy.getGroupByExpressions()) {
                if (groupByExpression instanceof net.sf.jsqlparser.expression.Function) {
                    featureConsumer.accept(groupByExpression,
                            metadata.getFeatureNow(
                                    FeatureId.GroupBy.of(((net.sf.jsqlparser.expression.Function) groupByExpression).getName())
                                    , groupByExpression::toString));
                } else if (groupByExpression instanceof Column) {
                    featureConsumer.accept(groupByExpression, metadata.getFeatureNow(FeatureId.GroupBy.property));
                } else if (groupByExpression instanceof BinaryExpression) {
                    featureConsumer.accept(groupByExpression,
                            metadata.getFeatureNow(FeatureId.GroupBy.of(((BinaryExpression) groupByExpression).getStringExpression()), groupByExpression::toString));
                } else {
                    throw new UnsupportedOperationException("不支持的分组表达式:" + groupByExpression);
                }
            }

            Function<Flux<ReactorQLRecord>, Flux<? extends Flux<ReactorQLRecord>>> groupMapper = groupByRef.get();
            if (groupMapper != null) {
                Expression having = select.getHaving();
                if (null != having) {
                    BiFunction<ReactorQLRecord, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(having, metadata);
                    return flux -> groupMapper
                            .apply(flux)
                            .flatMap(group -> columnMapper
                                    .apply(group)
                                    .filterWhen(ctx -> filter.apply(ctx, ctx.getRecord())));
                }
                return flux -> groupMapper.apply(flux)
                        .flatMap(group -> columnMapper.apply(group));
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

    protected Optional<Function<ReactorQLRecord, ? extends Publisher<?>>> createExpressionMapper(Expression expression) {
        return ValueMapFeature.createMapperByExpression(expression, metadata);
    }

    protected Optional<Function<Flux<ReactorQLRecord>, Flux<Object>>> createAggMapper(Expression expression) {

        AtomicReference<Function<Flux<ReactorQLRecord>, Flux<Object>>> ref = new AtomicReference<>();

        Consumer<ValueAggMapFeature> featureConsumer = feature -> {
            Function<Flux<ReactorQLRecord>, Flux<Object>> mapper = feature.createMapper(expression, metadata);
            ref.set(mapper);
        };
        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            metadata.getFeature(FeatureId.ValueAggMap.of(((net.sf.jsqlparser.expression.Function) expression).getName()))
                    .ifPresent(featureConsumer);
        }
        return Optional.ofNullable(ref.get());

    }

    private Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper() {

        Map<String, Function<ReactorQLRecord, ? extends Publisher<?>>> mappers = new LinkedHashMap<>();

        Map<String, Function<Flux<ReactorQLRecord>, Flux<Object>>> aggMapper = new LinkedHashMap<>();

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

                    if (!mappers.containsKey(alias) && !aggMapper.containsKey(alias)) {
                        throw new UnsupportedOperationException("不支持的操作:" + expression);
                    }
                }
            });
        }
        //转换结果集
        Function<ReactorQLRecord, Mono<ReactorQLRecord>> resultMapper = ctx ->
                Flux.fromIterable(mappers.entrySet())
                        .flatMap(e -> Mono.zip(Mono.just(e.getKey()), Mono.from(e.getValue().apply(ctx))))
                        .doOnNext(tp2 -> ctx.setResult(tp2.getT1(), tp2.getT2()))
                        .then()
                        .thenReturn(ctx);
        //聚合结果
        if (!aggMapper.isEmpty()) {
            return flux -> flux
                    .collectList()
                    .flatMap(list -> {
                        ReactorQLRecord first = list.isEmpty()
                                ? newContext(null, new HashMap<>(), new DefaultReactorQLContext((r) -> Flux.just(1)))
                                : list.get(0);
                        Flux<ReactorQLRecord> rows = Flux.fromIterable(list);
                        return Flux.fromIterable(aggMapper.entrySet())
                                .flatMap(e -> {
                                    String name = e.getKey();
                                    return e.getValue().apply(rows)
                                            .zipWith(Mono.just(name));
                                })
                                .collectMap(Tuple2::getT2, Tuple2::getT1)
                                .flatMap(map -> {
                                    ReactorQLRecord newCtx = first.resultToRecord().setResults(map);
                                    if (!mappers.isEmpty()) {
                                        return resultMapper.apply(newCtx);
                                    }
                                    return Mono.just(newCtx);
                                });

                    }).flux();
        }
        //指定了分组,但是没有聚合.只获取一个结果.
        if (metadata.getSql().getGroupBy() != null) {
            return flux -> flux.takeLast(1).flatMap(resultMapper);
        }
        return flux -> flux.flatMap(resultMapper);
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

    @Override
    public Flux<ReactorQLRecord> start(ReactorQLContext context) {
        return builder
                .apply(context);
    }


    @Override
    public Flux<Object> start(Function<String, Publisher<?>> streamSupplier) {
        return start(new DefaultReactorQLContext(t -> Flux.from(streamSupplier.apply(t))))
                .map(ReactorQLRecord::asMap);
    }


}
