package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.Date;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class AbstractFilterFeature implements FilterFeature {

    private String id;

    public AbstractFilterFeature(String type) {
        this.id = FeatureId.Filter.of(type).getId();
    }

    @Override
    public Predicate<Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        GreaterThan greaterThan = ((GreaterThan) expression);

        Expression left = greaterThan.getLeftExpression();
        Expression right = greaterThan.getRightExpression();

        Function<Object, Object> leftMapper = createValeMapper(left, metadata);
        Function<Object, Object> rightMapper = createValeMapper(right, metadata);
        return v -> predicate(leftMapper.apply(v), rightMapper.apply(v));
    }

    private boolean predicate(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return doPredicate(((Number) left), ((Number) right));
        }
        if (left instanceof Date && right instanceof Date) {
            return doPredicate(((Date) left), ((Date) right));
        }
        if (left instanceof String && right instanceof String) {
            return doPredicate(((String) left), ((String) right));
        }
        return doPredicate(left, right);
    }

    protected abstract boolean doPredicate(Number left, Number right);

    protected abstract boolean doPredicate(Date left, Date right);

    protected abstract boolean doPredicate(String left, String right);

    protected abstract boolean doPredicate(Object left, Object right);

    protected Function<Object, Object> createValeMapper(Expression expr, ReactorQLMetadata metadata) {
        if (expr instanceof Column) {
            return metadata.getFeatureNow(FeatureId.ValueMap.of("property"))
                    .createMapper(expr, metadata);
        }
        if (expr instanceof net.sf.jsqlparser.expression.Function) {
            return metadata.getFeatureNow(FeatureId.ValueMap.of(((net.sf.jsqlparser.expression.Function) expr).getName()))
                    .createMapper(expr, metadata);
        }
        if (expr instanceof StringValue) {
            String val = ((StringValue) expr).getValue();
            return v -> val;
        }

        if (expr instanceof LongValue) {
            long val = ((LongValue) expr).getValue();
            return v -> val;
        }

        if (expr instanceof DoubleValue) {
            double val = ((DoubleValue) expr).getValue();
            return v -> val;
        }

        if (expr instanceof DateValue) {
            Date val = ((DateValue) expr).getValue();
            return v -> val;
        }

        return v -> v;
    }

    @Override
    public String getId() {
        return id;
    }
}
