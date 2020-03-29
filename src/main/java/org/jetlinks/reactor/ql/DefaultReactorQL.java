package org.jetlinks.reactor.ql;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.feature.*;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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

    Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> where;
    Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> columnMapper;
    Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> limit;
    Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> offset;
    Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> groupBy;

    Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> join;


    Function<Function<String, Flux<Object>>, Flux<ReactorQLContext>> builder;

    private ReactorQLContext newContext(String name, Object row, Function<String, Flux<Object>> supplier) {
        if (row instanceof ReactorQLContext) {
            return ((ReactorQLContext) row);//.addRecord(name,((ReactorQLContext) row).getRecord());
        }
        return new DefaultReactorQLContext(name, row, supplier);
    }

    private Flux<ReactorQLContext> mapContext(String name, Function<String, Flux<Object>> supplier, Flux<Object> flux) {
        return flux.map(obj -> newContext(name, obj, supplier));
    }

    protected void prepare() {
        where = createWhere();
        columnMapper = createMapper();
        limit = createLimit();
        offset = createOffset();
        groupBy = createGroupBy();
        join = createJoin();

        PlainSelect select = metadata.getSql();
        FromItem from = select.getFromItem();

        if (from == null) {
            if (null != select.getGroupBy()) {
                builder = supplier ->
                        limit.apply(offset.apply(
                                groupBy.apply(
                                        where.apply(
                                                join.apply(supplier.apply(null).as(flx -> mapContext(null, supplier, flx)))))
                                )
                        );
            } else {
                builder = supplier ->
                        limit.apply(
                                offset.apply(
                                        columnMapper.apply(
                                                where.apply(
                                                        join.apply(supplier.apply(null).as(flx -> mapContext(null, supplier, flx))))
                                        )
                                )
                        );
            }
        } else if (from instanceof Table) {
            Table table = (Table) from;
            String tableName = table.getName();
            String alias = table.getAlias() != null ? table.getAlias().getName() : tableName;
            if (null != select.getGroupBy()) {
                builder = supplier -> limit.apply(offset.apply(groupBy.apply(where.apply(join.apply(supplier.apply(tableName).as(flx -> mapContext(alias, supplier, flx)))))));
            } else {
                builder = supplier -> limit.apply(offset.apply(columnMapper.apply(where.apply(join.apply(supplier.apply(tableName).as(flx -> mapContext(alias, supplier, flx)))))));
            }
        } else {
            SelectBody body = null;
            if (from instanceof SubSelect) {
                body = (((SubSelect) from).getSelectBody());
            }
            if (body instanceof PlainSelect) {
                String name = from.getAlias() != null ? from.getAlias().getName() : null;
                PlainSelect plainSelect = ((PlainSelect) body);
                DefaultReactorQL child = new DefaultReactorQL(new DefaultReactorQLMetadata(plainSelect));
                if (null != select.getGroupBy()) {
                    builder = ctx -> limit.apply(offset.apply(groupBy.apply(where.apply(join.apply(child.builder.apply(ctx).map(v -> v.resultToRecord(name)))))));
                } else {
                    builder = ctx -> limit.apply(offset.apply(columnMapper.apply(where.apply(join.apply(child.builder.apply(ctx).map(v -> v.resultToRecord(name)))))));
                }
            }
        }
        if (builder == null) {
            throw new UnsupportedOperationException("不支持的SQL语句:" + select);
        }
    }

    static Mono<Boolean> alwaysTrue = Mono.just(true);


    protected Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> createJoin() {
        if (CollectionUtils.isEmpty(metadata.getSql().getJoins())) {
            return Function.identity();
        }
        Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>>
                mapper = Function.identity();
        for (Join joinInfo : metadata.getSql().getJoins()) {
            Expression on = joinInfo.getOnExpression();
            FromItem from = joinInfo.getRightItem();
            BiFunction<ReactorQLContext, Object, Mono<Boolean>> filter;
            if (on == null) {
                filter = (ctx, v) -> alwaysTrue;
            } else {
                filter = FilterFeature.createPredicateNow(on, metadata);
            }

            Function<ReactorQLContext, Flux<ReactorQLContext>> rightStreamGetter = null;

            //join (select deviceId,avg(temp) from temp group by interval('10s'),deviceId )
            if (from instanceof SubSelect) {
                String alias = from.getAlias() == null ? null : from.getAlias().getName();
                DefaultReactorQL ql = new DefaultReactorQL(new DefaultReactorQLMetadata(((PlainSelect) ((SubSelect) from).getSelectBody())));
                rightStreamGetter = ctx -> ql.builder
                        .apply(name -> ctx.getDataSource(name)
                                .map(v -> newContext(name, v, ctx.getDataSourceSupplier())
                                        .addRecord(ctx.getName(), ctx.getRecord())))
                        .map(v -> ctx.addRecord(alias, v.asMap()));

            } else if ((from instanceof Table)) {
                String name = ((Table) from).getFullyQualifiedName();
                String alias = from.getAlias() == null ? name : from.getAlias().getName();
                rightStreamGetter = ctx -> ctx.getDataSource(name)
                        .map(right -> newContext(alias,right,ctx.getDataSourceSupplier()).addRecord(ctx.getName(), ctx.getRecord()));
            }
            if (rightStreamGetter == null) {
                throw new UnsupportedOperationException("不支持的表关联: " + from);
            }
            Function<ReactorQLContext, Flux<ReactorQLContext>> fiRightStreamGetter = rightStreamGetter;
            if (joinInfo.isLeft()) {
                mapper = mapper.andThen(flux ->
                        flux.flatMap(left -> fiRightStreamGetter
                                .apply(left)
                                .filterWhen(right -> filter.apply(right, right.getRecord()))
                                .defaultIfEmpty(left)));
            }   else {
                mapper = mapper.andThen(flux ->
                        flux.flatMap(left -> fiRightStreamGetter.apply(left).filterWhen(v -> filter.apply(v, v.getRecord())))
                );
            }
        }
        return mapper;
    }

    protected Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> createGroupBy() {
        PlainSelect select = metadata.getSql();
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
            AtomicReference<Function<Flux<ReactorQLContext>, Flux<? extends Flux<ReactorQLContext>>>> groupByRef = new AtomicReference<>();
            BiConsumer<Expression, GroupFeature> featureConsumer = (expr, feature) -> {
                Function<Flux<ReactorQLContext>, Flux<? extends Flux<ReactorQLContext>>> mapper = feature.createGroupMapper(expr, metadata);
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

            Function<Flux<ReactorQLContext>, Flux<? extends Flux<ReactorQLContext>>> groupMapper = groupByRef.get();
            if (groupMapper != null) {
                Expression having = select.getHaving();
                if (null != having) {
                    BiFunction<ReactorQLContext, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(having, metadata);
                    return flux -> groupMapper
                            .apply(flux)
                            .flatMap(group -> columnMapper
                                    .apply(group)
                                    .filterWhen(v -> filter.apply(v, v)));
                }
                return flux -> groupMapper.apply(flux)
                        .flatMap(group -> columnMapper.apply(group));
            }
        }
        return Function.identity();

    }

    protected Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> createWhere() {
        Expression whereExpr = metadata.getSql().getWhere();
        if (whereExpr == null) {
            return Function.identity();
        }
        BiFunction<ReactorQLContext, Object, Mono<Boolean>> filter = FilterFeature.createPredicateNow(whereExpr, metadata);
        return flux -> flux.filterWhen(v -> filter.apply(v, v));
    }

    protected Optional<Function<ReactorQLContext, ? extends Publisher<?>>> createExpressionMapper(Expression expression) {
        return ValueMapFeature.createMapperByExpression(expression, metadata);
    }

    protected Optional<Function<Flux<ReactorQLContext>, Flux<Object>>> createAggMapper(Expression expression) {

        AtomicReference<Function<Flux<ReactorQLContext>, Flux<Object>>> ref = new AtomicReference<>();

        Consumer<ValueAggMapFeature> featureConsumer = feature -> {
            Function<Flux<ReactorQLContext>, Flux<Object>> mapper = feature.createMapper(expression, metadata);
            ref.set(mapper);
        };
        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            metadata.getFeature(FeatureId.ValueAggMap.of(((net.sf.jsqlparser.expression.Function) expression).getName()))
                    .ifPresent(featureConsumer);
        }
        return Optional.ofNullable(ref.get());

    }

    private Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> createMapper() {

        Map<String, Function<ReactorQLContext, ? extends Publisher<?>>> mappers = new LinkedHashMap<>();

        Map<String, Function<Flux<ReactorQLContext>, Flux<Object>>> aggMapper = new LinkedHashMap<>();

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
        Function<ReactorQLContext, Mono<ReactorQLContext>> resultMapper = ctx ->
                Flux.fromIterable(mappers.entrySet())
                        .flatMap(e -> Mono.zip(Mono.just(e.getKey()), Mono.from(e.getValue().apply(ctx))))
                        .doOnNext(tp2 -> ctx.setValue(tp2.getT1(), tp2.getT2()))
                        .then()
                        .thenReturn(ctx);
        //聚合结果
        if (!aggMapper.isEmpty()) {
            return flux -> flux
                    .collectList()
                    .flatMap(list -> {
                        ReactorQLContext first = list.get(0);
                        //newContext(new ConcurrentHashMap<>(), ((DefaultReactorQLContext) list.get(0)).ctxSupplier);
                        Flux<ReactorQLContext> rows = Flux.fromIterable(list);
                        return Flux.fromIterable(aggMapper.entrySet())
                                .flatMap(e -> {
                                    String name = e.getKey();
                                    return e.getValue().apply(rows)
                                            .zipWith(Mono.just(name));
                                })
                                .collectMap(Tuple2::getT2, Tuple2::getT1)
                                .flatMap(map -> {
                                    ReactorQLContext newCtx = first.resultToRecord();
                                    newCtx.setValues(map);
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

    private Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> createLimit() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getRowCount();
            if (expr instanceof LongValue) {
                return flux -> flux.take(((LongValue) expr).getValue());
            }
        }
        return Function.identity();
    }

    private Function<Flux<ReactorQLContext>, Flux<ReactorQLContext>> createOffset() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getOffset();
            if (expr instanceof LongValue) {
                return flux -> flux.skip(((LongValue) expr).getValue());
            }
        }
        return Function.identity();
    }

    protected Flux<Object> doStart(Function<String, Publisher<?>> streamSupplier) {
        return builder.apply(table -> Flux.from(streamSupplier.apply(table)))
                .map(ReactorQLContext::asMap);
    }

    @Override
    public Flux<Object> start(Function<String, Publisher<?>> streamSupplier) {
        return doStart(streamSupplier);
    }


}
