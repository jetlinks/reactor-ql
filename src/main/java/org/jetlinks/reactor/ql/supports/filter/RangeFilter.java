package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class RangeFilter implements FilterFeature {

    private static final String ID = FeatureId.Filter.of("range").getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> expr;
        if (function.getParameters() == null || CollectionUtils.isEmpty(expr = function.getParameters().getExpressions()) || expr.size() != 3) {
            throw new IllegalArgumentException("函数参数数量必须为3:" + function);
        }
        Expression left = expr.get(0);
        Expression between = expr.get(1);
        Expression and = expr.get(2);

        return BetweenFilter.doCreate(left, between, and, metadata, false);
    }

    @Override
    public String getId() {
        return ID;
    }
}
