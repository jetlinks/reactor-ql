package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.util.function.Tuple2;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.jetlinks.reactor.ql.feature.FeatureId.ValueMap.createValeMapperNow;

public class CalculateMapFeature implements ValueMapFeature {

    @Getter
    private String id;

    private Function<Object, Object> calculator;

    public CalculateMapFeature(String type, Function<Object, Object> calculator) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.calculator = calculator;
    }

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> expressions;
        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions())) {
            throw new UnsupportedOperationException("函数必须指定参数:" + expression);
        }

        Function<Object, Object> mapper = createValeMapperNow(expressions.get(0), metadata);

        return v -> calculator.apply(mapper.apply(v));
    }


}
