package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;

import java.util.function.BiPredicate;

public class OrFilter implements FilterFeature {

    static String id = FeatureId.Filter.or.getId();

    @Override
    public BiPredicate<Object,Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        OrExpression or = ((OrExpression) expression);

        Expression left = or.getLeftExpression();
        Expression right = or.getRightExpression();

        BiPredicate<Object,Object> leftPredicate = FeatureId.Filter.createPredicate(left, metadata);
        BiPredicate<Object,Object> rightPredicate = FeatureId.Filter.createPredicate(right, metadata);
        return leftPredicate.or(rightPredicate);
    }

    @Override
    public String getId() {
        return id;
    }
}
