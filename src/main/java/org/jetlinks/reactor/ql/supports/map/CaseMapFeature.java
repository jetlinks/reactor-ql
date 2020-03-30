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
                switchExpr == null ? v -> Mono.just(v.getRecord()) :
                        ValueMapFeature.createMapperNow(switchExpr, metadata);

        Map<BiFunction<ReactorQLContext, Object, Mono<Boolean>>, Function<ReactorQLContext, ? extends Publisher<?>>> cases = new LinkedHashMap<>();
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
            Expression when = whenClause.getWhenExpression();
            Expression then = whenClause.getThenExpression();
            cases.put(createWhen(when, metadata), createThen(then, metadata));
        }
        Expression elseExpr = caseExpression.getElseExpression();
        Function<ReactorQLContext, ? extends Publisher<?>> fElse = createThen(elseExpr, metadata);

        return ctx -> {
            Mono<?> switchValue = Mono.from(valueMapper.apply(ctx));
            return Flux.fromIterable(cases.entrySet())
                    .filterWhen(e -> switchValue.flatMap(v -> e.getKey().apply(ctx, v)))
                    .flatMap(e -> e.getValue().apply(ctx))
                    .switchIfEmpty((Publisher) fElse.apply(ctx));
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
