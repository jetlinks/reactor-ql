package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;

import java.util.function.BiPredicate;

public class AndFilter implements FilterFeature {

    static String id = FeatureId.Filter.and.getId();

    @Override
    public BiPredicate<Object, Object> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        AndExpression and = ((AndExpression) expression);

        Expression left = and.getLeftExpression();
        Expression right = and.getRightExpression();

        BiPredicate<Object, Object> leftPredicate =FilterFeature.createPredicateNow(left, metadata);
        BiPredicate<Object, Object> rightPredicate = FilterFeature.createPredicateNow(right, metadata);
        return leftPredicate.and(rightPredicate);
    }


    @Override
    public String getId() {
        return id;
    }
}
