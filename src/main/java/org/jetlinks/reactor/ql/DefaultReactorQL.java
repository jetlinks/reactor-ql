package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

public class DefaultReactorQL implements ReactorQL {

    private Function<String, Publisher<?>> streamSupplier;


    public DefaultReactorQL(ReactorQLMetadata metadata) {
        this.metadata = metadata;
    }

    private ReactorQLMetadata metadata;


    @Override
    public ReactorQL source(Function<String, Publisher<?>> streamSupplier) {
        this.streamSupplier = streamSupplier;
        return this;
    }

    @Override
    public ReactorQL source(Publisher<?> publisher) {
        return source((n) -> publisher);
    }


    protected Flux<Object> applyWhere(Flux<Object> flux) {

        AtomicReference<Predicate<Object>> where = new AtomicReference<>();

        Expression whereExpr = metadata.getSql().getWhere();
        if (whereExpr == null) {
            return flux;
        }
        if (whereExpr instanceof ComparisonOperator) {
            metadata.getFeature(FeatureId.Filter.of(((ComparisonOperator) whereExpr).getStringExpression()))
                    .ifPresent(filterFeature -> where.set(filterFeature.createMapper(whereExpr, metadata)));
        }
        whereExpr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(AndExpression expr) {
                metadata.getFeature(FeatureId.Filter.of("and"))
                        .ifPresent(filterFeature -> where.set(filterFeature.createMapper(expr, metadata)));
            }

            @Override
            public void visit(OrExpression expr) {
                metadata.getFeature(FeatureId.Filter.of("or"))
                        .ifPresent(filterFeature -> where.set(filterFeature.createMapper(expr, metadata)));
            }

            @Override
            public void visit(Between expr) {
                metadata.getFeature(FeatureId.Filter.of("between"))
                        .ifPresent(filterFeature -> where.set(filterFeature.createMapper(expr, metadata)));
            }
        });
        if (where.get() != null) {
            return flux.filter(where.get());
        }
        return flux;
    }

    protected Flux<Object> applyMap(Flux<Object> flux) {

        Map<String, Function<Object, Object>> mappers = new HashMap<>();

        Map<String, Function<Flux<Object>, Mono<? extends Number>>> aggMapper = new HashMap<>();

        for (SelectItem selectItem : metadata.getSql().getSelectItems()) {
            selectItem.accept(new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectExpressionItem item) {

                    Expression expression = item.getExpression();
                    String alias = item.getAlias() == null ? expression.toString() : item.getAlias().getName();
                    expression.accept(new ExpressionVisitorAdapter() {

                        @Override
                        public void visit(net.sf.jsqlparser.expression.Function function) {

                            metadata.getFeature(FeatureId.ValueAggMap.of(function.getName()))
                                    .ifPresent(feature -> aggMapper.put(alias, feature.createMapper(function, metadata)));

                            metadata.getFeature(FeatureId.ValueMap.of(function.getName()))
                                    .ifPresent(feature -> mappers.put(alias, feature.createMapper(function, metadata)));

                        }

                        @Override
                        public void visit(CaseExpression expr) {
                            metadata.getFeature(FeatureId.ValueMap.of("case"))
                                    .ifPresent(feature -> mappers.put(alias, feature.createMapper(expr, metadata)));
                        }

                        @Override
                        public void visit(Concat expr) {
                            metadata.getFeature(FeatureId.ValueMap.of("concat"))
                                    .ifPresent(feature -> mappers.put(alias, feature.createMapper(expr, metadata)));
                        }

                        @Override
                        public void visit(Column column) {
                            metadata.getFeature(FeatureId.ValueMap.of("property"))
                                    .ifPresent(feature -> mappers.put(alias, feature.createMapper(column, metadata)));
                        }

                        @Override
                        public void visit(StringValue value) {
                            mappers.put(alias, (v) -> value.getValue());
                        }

                        @Override
                        public void visit(LongValue value) {
                            mappers.put(alias, (v) -> value.getValue());
                        }
                    });
                }
            });
        }
        if (!aggMapper.isEmpty()) {
            return flux
                    .collectList()
                    .<Object>flatMap(list ->
                            Flux.fromIterable(aggMapper.entrySet())
                                    .flatMap(e -> Mono.zip(Mono.just(e.getKey()), e.getValue().apply(Flux.fromIterable(list))))
                                    .<String, Object>collectMap(Tuple2::getT1, Tuple2::getT2)
                                    .doOnNext(map -> {
                                        if (!mappers.isEmpty()) {
                                            for (Map.Entry<String, Function<Object, Object>> mapper : mappers.entrySet()) {
                                                map.put(mapper.getKey(), mapper.getValue().apply(list.get(0)));
                                            }
                                        }
                                    }))
                    .flux();
        }
        return flux
                .map(obj -> {
                    Map<String, Object> value = new HashMap<>();
                    for (Map.Entry<String, Function<Object, Object>> mapper : mappers.entrySet()) {
                        value.put(mapper.getKey(), mapper.getValue().apply(obj));
                    }
                    return value;
                });
    }

    protected Flux<Object> applyLimit(Flux<Object> flux) {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getRowCount();
            if (expr instanceof LongValue) {
                return flux.take(((LongValue) expr).getValue());
            }
        }
        return flux;
    }

    protected Flux<Object> applyOffset(Flux<Object> flux) {
        Limit limit = metadata.getSql().getLimit();
        if (limit != null) {
            Expression expr = limit.getOffset();
            if (expr instanceof LongValue) {
                return flux.skip(((LongValue) expr).getValue());
            }
        }
        return flux;
    }

    protected Flux<Object> process() {
        PlainSelect select = metadata.getSql();
        Table table = (Table) select.getFromItem();
        Flux<Object> main = applyOffset(Flux.from(streamSupplier.apply(table.getName())));
        GroupByElement groupBy = select.getGroupBy();
        if (null != groupBy) {
            AtomicReference<Flux<GroupedFlux<Object, Object>>> groupByRef = new AtomicReference<>();
            for (Expression groupByExpression : groupBy.getGroupByExpressions()) {
                groupByExpression.accept(new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(net.sf.jsqlparser.expression.Function function) {
                        metadata.getFeature(FeatureId.GroupBy.of(function.getName()))
                                .ifPresent(feature -> groupByRef.set(feature.apply(main, function, metadata)));
                    }
                });
            }
            if (groupByRef.get() != null) {
                return applyLimit(groupByRef
                        .get()
                        .flatMap(group -> applyMap(applyWhere(group))));
            }
        }
        return applyLimit(applyMap(applyWhere(main)));
    }

    @Override
    public Flux<Object> start() {
        return process();
    }


}
