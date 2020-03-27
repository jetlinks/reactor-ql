package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

public class CaseMapFeature implements ValueMapFeature {

    static String ID = FeatureId.ValueMap.caseWhen.getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CaseExpression caseExpression = ((CaseExpression) expression);


        Expression switchExpr = caseExpression.getSwitchExpression();

        Function<Object, Object> valueMapper = FeatureId.ValueMap.createValeMapperNow(switchExpr, metadata);

        Map<BiPredicate<Object, Object>, Function<Object, Object>> cases = new LinkedHashMap<>();
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
            Expression when = whenClause.getWhenExpression();
            Expression then = whenClause.getThenExpression();
            cases.put(createWhen(when, metadata), createThen(then, metadata));
        }

        Expression elseExpr = caseExpression.getElseExpression();
        Function<Object, Object> fElse = createThen(elseExpr, metadata);
        return row -> {
            Object column = valueMapper.apply(row);
            for (Map.Entry<BiPredicate<Object, Object>, Function<Object, Object>> e : cases.entrySet()) {
                if (e.getKey().test(row, column)) {
                    return e.getValue().apply(row);
                }
            }
            return fElse.apply(row);
        };
    }

    protected Function<Object, Object> createThen(Expression expression, ReactorQLMetadata metadata) {

        return FeatureId.ValueMap.createValeMapperNow(expression, metadata);
    }

    protected BiPredicate<Object, Object> createWhen(Expression expression, ReactorQLMetadata metadata) {
        return FeatureId.Filter.createPredicate(expression, metadata);
    }

    @Override
    public String getId() {
        return ID;
    }
}
