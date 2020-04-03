package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InFilter implements FilterFeature {

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {

        InExpression inExpression = ((InExpression) expression);

        Expression left = inExpression.getLeftExpression();

        ItemsList in = (inExpression.getRightItemsList());

        List<Function<ReactorQLRecord, ? extends Publisher<?>>> rightMappers = new ArrayList<>();

        if (in instanceof ExpressionList) {
            rightMappers.addAll(((ExpressionList) in).getExpressions().stream()
                    .map(exp -> ValueMapFeature.createMapperNow(exp, metadata))
                    .collect(Collectors.toList()));
        }
        if (in instanceof SubSelect) {
            rightMappers.add(ValueMapFeature.createMapperNow(((SubSelect) in), metadata));
        }

        Function<ReactorQLRecord, ? extends Publisher<?>> leftMapper = ValueMapFeature.createMapperNow(left, metadata);

        return (ctx, column) ->
                doPredicate(
                        asFlux(leftMapper.apply(ctx)),
                        asFlux(Flux.fromIterable(rightMappers).flatMap(mapper -> mapper.apply(ctx)))
                );
    }

    protected Flux<Object> asFlux(Publisher<?> publisher) {
        return Flux.from(publisher)
                .flatMap(v -> {
                    if (v instanceof Iterable) {
                        return Flux.fromIterable(((Iterable<?>) v));
                    }
                    if (v instanceof Publisher) {
                        return ((Publisher<?>) v);
                    }
                    if (v instanceof Map && ((Map<?, ?>) v).size() == 1) {
                        return Mono.just(((Map<?, ?>) v).values().iterator().next());
                    }
                    return Mono.just(v);
                });
    }

    protected Mono<Boolean> doPredicate(Flux<Object> left, Flux<Object> values) {
        return values
                .flatMap(v -> left.map(l -> CompareUtils.equals(v, l)))
                .any(Boolean.TRUE::equals);
    }

    @Override
    public String getId() {
        return FeatureId.Filter.in.getId();
    }
}
