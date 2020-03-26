package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.beanutils.BeanUtilsBean2;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class CaseMapFeature implements ValueMapFeature {

    static String ID = FeatureId.ValueMap.of("case").getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CaseExpression caseExpression = ((CaseExpression) expression);


        Expression switchExpr = caseExpression.getSwitchExpression();

        Function<Object, Object> valueMapper = null;
        if (switchExpr instanceof Column) {
            valueMapper = metadata.getFeatureNow(FeatureId.ValueMap.of("property"))
                    .createMapper(switchExpr, metadata);
        } else if (switchExpr instanceof net.sf.jsqlparser.expression.Function) {
            valueMapper = metadata.getFeatureNow(FeatureId.ValueMap.of(((net.sf.jsqlparser.expression.Function) switchExpr).getName()))
                    .createMapper(switchExpr, metadata);
        }
        if (valueMapper == null) {
            throw new UnsupportedOperationException("unsupported switch expression:" + switchExpr);
        }

        Map<Predicate<Object>, BiFunction<Object, Object, Object>> cases = new LinkedHashMap<>();
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
            Expression when = whenClause.getWhenExpression();
            Expression then = whenClause.getThenExpression();
            cases.put(createWhen(when), createThen(then, metadata));
        }

        Expression elseExpr = caseExpression.getElseExpression();
        Function<Object, Object> fMapper = valueMapper;
        BiFunction<Object, Object, Object> fElse = createThen(elseExpr, metadata);
        return obj -> {
            Object value = fMapper.apply(obj);
            for (Map.Entry<Predicate<Object>, BiFunction<Object, Object, Object>> e : cases.entrySet()) {
                if (e.getKey().test(value)) {
                    return e.getValue().apply(obj, value);
                }
            }
            return fElse.apply(obj, value);
        };
    }

    protected BiFunction<Object, Object, Object> createThen(Expression expression, ReactorQLMetadata metadata) {
        if (expression instanceof NullValue) {
            return (v1, v2) -> null;
        }

        if (expression instanceof StringValue) {
            String val = ((StringValue) expression).getValue();
            return (v1, v2) -> val;
        }

        if (expression instanceof LongValue) {
            long val = ((LongValue) expression).getValue();
            return (v1, v2) -> val;
        }

        if (expression instanceof DoubleValue) {
            double val = ((DoubleValue) expression).getValue();
            return (v1, v2) -> val;
        }

        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            Function<Object, Object> mapper = metadata.getFeatureNow(FeatureId.ValueMap.of(((net.sf.jsqlparser.expression.Function) expression).getName()))
                    .createMapper(expression, metadata);
            return (v1, v2) -> mapper.apply(v1);
        }
        return (v1, v2) -> v2;
    }

    protected Predicate<Object> createWhen(Expression expression) {
        if (expression instanceof NullValue) {
            return Objects::isNull;
        }
        if (expression instanceof StringValue) {
            String strVal = ((StringValue) expression).getValue();
            return strVal::equals;
        }
        if (expression instanceof LongValue) {
            long val = ((LongValue) expression).getValue();
            return v -> {
                if (v instanceof Number) {
                    return val == ((Number) v).longValue();
                }
                return false;
            };
        }
        if (expression instanceof DoubleValue) {
            double val = ((DoubleValue) expression).getValue();
            return v -> {
                if (v instanceof Number) {
                    return val == ((Number) v).doubleValue();
                }
                return false;
            };
        }

        return (r) -> false;
    }

    @Override
    public String getId() {
        return ID;
    }
}
