package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.utils.CompareUtils;

import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class InFilter implements FilterFeature {

    @Override
    public BiPredicate<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {

        InExpression inExpression = ((InExpression) expression);

        Expression left = inExpression.getLeftExpression();

        ExpressionList in = ((ExpressionList) inExpression.getRightItemsList());

        List<Function<Object, Object>> mappers = in.getExpressions().stream()
                .map(exp -> FeatureId.ValueMap.createValeMapperNow(exp, metadata))
                .collect(Collectors.toList());

        Function<Object, Object> leftMapper = FeatureId.ValueMap.createValeMapperNow(left, metadata);

        return (row, column) -> doPredicate(leftMapper.apply(row), mappers.stream().map(mapper -> mapper.apply(row)));
    }

    protected boolean doPredicate(Object left, Stream<Object> values) {
        return values
                .flatMap(v -> {
                    if (v instanceof Iterable) {
                        return StreamSupport.stream(((Iterable<?>) v).spliterator(), false);
                    }
                    return Stream.of(v);
                })
                .anyMatch(r -> CompareUtils.compare(left, r));
    }

    @Override
    public String getId() {
        return FeatureId.Filter.in.getId();
    }
}
