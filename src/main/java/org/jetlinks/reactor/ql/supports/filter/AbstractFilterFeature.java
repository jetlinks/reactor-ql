package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.Date;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static org.jetlinks.reactor.ql.feature.FeatureId.ValueMap.createValeMapperNow;

public abstract class AbstractFilterFeature implements FilterFeature {

    private String id;

    public AbstractFilterFeature(String type) {
        this.id = FeatureId.Filter.of(type).getId();
    }

    @Override
    public BiPredicate<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        ComparisonOperator greaterThan = ((ComparisonOperator) expression);

        Expression left = greaterThan.getLeftExpression();
        Expression right = greaterThan.getRightExpression();

        Function<Object, Object> leftMapper = createValeMapperNow(left, metadata);
        Function<Object, Object> rightMapper = createValeMapperNow(right, metadata);
        return (row, column) -> predicate(leftMapper.apply(row), rightMapper.apply(row));
    }

    private boolean predicate(Object left, Object right) {
        if (left instanceof Number || right instanceof Number) {
            return doPredicate(CastUtils.castNumber(left), CastUtils.castNumber(right));
        }
        if (left instanceof Date || right instanceof Date) {
            return doPredicate(CastUtils.castDate(left), CastUtils.castDate(right));
        }
        if (left instanceof String || right instanceof String) {
            return doPredicate(String.valueOf(left), String.valueOf(right));
        }
        return doPredicate(left, right);
    }

    protected abstract boolean doPredicate(Number left, Number right);

    protected abstract boolean doPredicate(Date left, Date right);

    protected abstract boolean doPredicate(String left, String right);

    protected abstract boolean doPredicate(Object left, Object right);


    @Override
    public String getId() {
        return id;
    }
}
