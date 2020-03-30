package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.*;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class CaseMapFeature implements ValueMapFeature {

    static String ID = FeatureId.ValueMap.caseWhen.getId();

    @Override
    @SuppressWarnings("all")
    public Function<ReactorQLContext, ? extends Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CaseExpression caseExpression = ((CaseExpression) expression);
        Expression switchExpr = caseExpression.getSwitchExpression();

        Function<ReactorQLContext, ? extends Publisher<?>> valueMapper =
                switchExpr == null
                        ? v -> Mono.just(v.getRecord()) //case when
                        : ValueMapFeature.createMapperNow(switchExpr, metadata); // case column when

        Map<BiFunction<ReactorQLContext, Object, Mono<Boolean>>, Function<ReactorQLContext, ? extends Publisher<?>>> cases = new LinkedHashMap<>();
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
            Expression when = whenClause.getWhenExpression();
            Expression then = whenClause.getThenExpression();
            cases.put(createWhen(when, metadata), createThen(then, metadata));
        }
        Function<ReactorQLContext, ? extends Publisher<?>> thenElse = createThen(caseExpression.getElseExpression(), metadata);

        return ctx -> {
            Mono<?> switchValue = Mono.from(valueMapper.apply(ctx));
            return Flux.fromIterable(cases.entrySet())
                    .filterWhen(whenAndThen -> switchValue.flatMap(v -> whenAndThen.getKey().apply(ctx, v)))
                    .flatMap(whenAndThen -> whenAndThen.getValue().apply(ctx))
                    .switchIfEmpty((Publisher) thenElse.apply(ctx));
        };
    }

    protected Function<ReactorQLContext, ? extends Publisher<?>> createThen(Expression expression, ReactorQLMetadata metadata) {
        if (expression == null) {
            return (ctx) -> Mono.empty();
        }
        return ValueMapFeature.createMapperNow(expression, metadata);
    }

    protected BiFunction<ReactorQLContext, Object, Mono<Boolean>> createWhen(Expression expression, ReactorQLMetadata metadata) {
        if (expression == null) {
            return (ctx, v) -> Mono.just(false);
        }
        return FilterFeature.createPredicateNow(expression, metadata);
    }

    @Override
    public String getId() {
        return ID;
    }
}
