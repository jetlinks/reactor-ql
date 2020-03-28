package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.function.Function;

class DefaultReactorQL implements ReactorQL {


    public DefaultReactorQL(ReactorQLMetadata metadata) {
        this.metadata = metadata;
        prepare();
    }

    private ReactorQLMetadata metadata;

    Function<Flux<Object>, Flux<Object>> where;
    Function<Flux<Object>, Flux<Object>> columnMapper;
    Function<Flux<Object>, Flux<Object>> limit;
    Function<Flux<Object>, Flux<Object>> offset;
    Function<Flux<Object>, Flux<Object>> groupBy;

    Function<Function<String, Flux<Object>>, Flux<Object>> builder;


    protected void prepare() {
        where = createWhere();
        columnMapper = createMapper();
        limit = createLimit();
        offset = createOffset();
        groupBy = createGroupBy();
        PlainSelect select = metadata.getSql();
        if (select.getFromItem() instanceof Table) {
            Table table = (Table) select.getFromItem();
            String tableName = table.getName();
            if (null != select.getGroupBy()) {
                builder = supplier -> limit.apply(offset.apply(groupBy.apply(where.apply(supplier.apply(tableName)))));
            } else {
                builder = supplier -> limit.apply(offset.apply(columnMapper.apply(where.apply(supplier.apply(tableName)))));
            }
        } else if (select.getFromItem() instanceof SubSelect) {
            DefaultReactorQL child = new DefaultReactorQL(new DefaultReactorQLMetadata(((PlainSelect) ((SubSelect) select.getFromItem()).getSelectBody())));
            if (null != select.getGroupBy()) {
                builder = supplier -> limit.apply(offset.apply(groupBy.apply(where.apply(child.builder.apply(supplier)))));
            } else {
                builder = supplier -> limit.apply(offset.apply(columnMapper.apply(where.apply(child.builder.apply(supplier)))));
            }
        }
        if (builder == null) {
            throw new UnsupportedOperationException("不支持的SQL语句:" + select);
        }

    }

    protected Function<Flux<Object>, Flux<Object>> createGroupBy() {
        PlainSelect select = metadata.getSql();
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
            AtomicReference<Function<Flux<Object>, Flux<? extends Flux<Object>>>> groupByRef = new AtomicReference<>();
            BiConsumer<Expression, GroupByFeature> featureConsumer = (expr, feature) -> {
                Function<Flux<Object>, Flux<? extends Flux<Object>>> mapper = feature.createMapper(expr, metadata);
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

            Function<Flux<Object>, Flux<? extends Flux<Object>>> groupMapper = groupByRef.get();
            if (groupMapper != null) {
                Expression having = select.getHaving();
                if (null != having) {
                    BiPredicate<Object, Object> filter = FeatureId.Filter.createPredicateNow(having, metadata);
                    return flux -> groupMapper
                            .apply(flux)
                            .flatMap(group -> columnMapper.apply(group)
                                    .filter(v -> filter.test(v, v)));
                }
                return flux -> groupMapper.apply(flux)
                        .flatMap(group -> columnMapper.apply(group));
            }
        }
        return Function.identity();

    }

    private Function<Flux<Object>, Flux<Object>> createWhere() {
        Expression whereExpr = metadata.getSql().getWhere();
        if (whereExpr == null) {
            return Function.identity();
        }
        BiPredicate<Object, Object> filter = FeatureId.Filter.createPredicateNow(whereExpr, metadata);
        return flux -> flux.filter(v -> filter.test(v, v));
    }

    protected Optional<Function<Object, Object>> createExpressionMapper(Expression expression) {
        return FeatureId.ValueMap.createValeMapper(expression, metadata);
    }

    protected Optional<Function<Flux<Object>, Flux<Object>>> createAggMapper(Expression expression) {

        AtomicReference<Function<Flux<Object>, Flux<Object>>> ref = new AtomicReference<>();

        Consumer<ValueAggMapFeature> featureConsumer = feature -> {
            Function<Flux<Object>, Flux<Object>> mapper = feature.createMapper(expression, metadata);
            if (ref.get() != null) {
                ref.set(ref.get().andThen(flux -> mapper.apply(flux).cast(Object.class)));
            } else {
                ref.set(flux -> mapper.apply(flux).cast(Object.class));
            }
        };
        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            metadata.getFeature(FeatureId.ValueAggMap.of(((net.sf.jsqlparser.expression.Function) expression).getName()))
                    .ifPresent(featureConsumer);
        }
        if (expression instanceof BinaryExpression) {
            // TODO: 2020/3/27
            //处理聚合运算

            //   BinaryExpression binary = ((BinaryExpression) expression);
            //    metadata.getFeatureNow(FeatureId.ValueAggMap.of(binary.getStringExpression()))

        }

        return Optional.ofNullable(ref.get());

    }

    private Function<Flux<Object>, Flux<Object>> createMapper() {

        Map<String, Function<Object, Object>> mappers = new LinkedHashMap<>();

        Map<String, Function<Flux<Object>, Flux<Object>>> aggMapper = new LinkedHashMap<>();

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
        Function<Object, Map<String, Object>> resultMapper = obj -> {
            Map<String, Object> value = new LinkedHashMap<>();
            for (Map.Entry<String, Function<Object, Object>> mapper : mappers.entrySet()) {
                value.put(mapper.getKey(), mapper.getValue().apply(obj));
            }
            return value;
        };
        //聚合结果
        if (!aggMapper.isEmpty()) {
            return flux -> flux
                    .collectList()
                    .<Object>flatMap(list ->
                            Flux.fromIterable(aggMapper.entrySet())
                                    .flatMap(e -> e.getValue().apply(Flux.fromIterable(list)).zipWith(Mono.just(e.getKey())))
                                    .collectMap(Tuple2::getT2, Tuple2::getT1, ConcurrentHashMap::new)
                                    .doOnNext(map -> {
                                        if (!mappers.isEmpty()) {
                                            map.putAll(resultMapper.apply(list.get(0)));
                                        }
                                    }))
                    .flux();
        }
        //指定了分组,但是没有聚合.只获取一个结果.
        if (metadata.getSql().getGroupBy() != null) {
            return flux -> flux.takeLast(1).map(resultMapper);
        }
        return flux -> flux.map(resultMapper);
    }

    private Function<Flux<Object>, Flux<Object>> createLimit() {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getRowCount();
            if (expr instanceof LongValue) {
                return flux -> flux.take(((LongValue) expr).getValue());
            }
        }
        return Function.identity();
    }

    private Function<Flux<Object>, Flux<Object>> createOffset() {
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
        return builder.apply(table -> Flux.from(streamSupplier.apply(table)));
    }

    @Override
    public Flux<Object> start(Function<String, Publisher<?>> streamSupplier) {
        return doStart(streamSupplier);
    }


}
