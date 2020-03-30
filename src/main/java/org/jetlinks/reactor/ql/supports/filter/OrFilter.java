package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;

public class OrFilter implements FilterFeature {

    static String id = FeatureId.Filter.or.getId();

    @Override
    public BiFunction<ReactorQLContext, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        OrExpression and = ((OrExpression) expression);

        Expression leftExpr = and.getLeftExpression();
        Expression rightExpr = and.getRightExpression();

        BiFunction<ReactorQLContext, Object, Mono<Boolean>> leftPredicate = FilterFeature.createPredicateNow(leftExpr, metadata);
        BiFunction<ReactorQLContext, Object, Mono<Boolean>> rightPredicate = FilterFeature.createPredicateNow(rightExpr, metadata);

        // a=1 or b=1
        return (ctx, val) -> Mono.zip(
                leftPredicate.apply(ctx, val),
                rightPredicate.apply(ctx, val),
                (leftVal, rightVal) -> leftVal || rightVal);
    }


    @Override
    public String getId() {
        return id;
    }
}
