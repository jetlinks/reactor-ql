package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class BetweenFilter implements FilterFeature {

    static String ID = FeatureId.Filter.between.getId();

    @Override
    public BiPredicate<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {

        Between betweenExpr = ((Between) expression);
        Expression left = betweenExpr.getLeftExpression();
        Expression between = betweenExpr.getBetweenExpressionStart();
        Expression and = betweenExpr.getBetweenExpressionEnd();

        Function<Object, Object> leftMapper = FeatureId.ValueMap.createValeMapperNow(left, metadata);
        Function<Object, Object> betweenMapper = FeatureId.ValueMap.createValeMapperNow(between, metadata);
        Function<Object, Object> andMapper = FeatureId.ValueMap.createValeMapperNow(and, metadata);
        boolean not = betweenExpr.isNot();

        return (row, column) -> not != predicate(leftMapper.apply(row), betweenMapper.apply(row), andMapper.apply(row));
    }

    protected boolean predicate(Object val, Object between, Object and) {
        if (val == null || between == null || and == null) {
            return false;
        }
        if (val.equals(between) || val.equals(and)) {
            return true;
        }
        if (val instanceof Date || between instanceof Date || and instanceof Date) {
            val = CastUtils.castDate(val);
            between = CastUtils.castDate(between);
            and = CastUtils.castDate(and);
        }
        if (val instanceof Number || between instanceof Number || and instanceof Number) {
            double doubleVal = CastUtils.castNumber(val).doubleValue();
            return doubleVal >= CastUtils.castNumber(between).doubleValue() && doubleVal <= CastUtils.castNumber(and).doubleValue();
        }

        Object[] arr = new Object[]{val, between, and};
        Arrays.sort(arr);
        return arr[1] == val;
    }

    @Override
    public String getId() {
        return ID;
    }
}
