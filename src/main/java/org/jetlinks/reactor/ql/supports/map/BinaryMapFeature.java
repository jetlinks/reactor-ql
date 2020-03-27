package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;

import java.math.BigDecimal;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.jetlinks.reactor.ql.feature.FeatureId.ValueMap.createValeMapperNow;

public class BinaryMapFeature implements ValueMapFeature {

    @Getter
    private String id;
    private String type;

    private BiFunction<Number,Number,Object> mapper;

    public BinaryMapFeature(String type, BiFunction<Number,Number,Object> mapper) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.type = type;
        this.mapper=mapper;
    }

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        BinaryExpression bie = ((BinaryExpression) expression);
        Expression left = bie.getLeftExpression();
        Expression right = bie.getRightExpression();
        Function<Object, Object> leftMapper = createValeMapperNow(left, metadata);
        Function<Object, Object> rightMapper = createValeMapperNow(right, metadata);
        return v -> calculate(leftMapper.apply(v), rightMapper.apply(v));
    }

    private Object calculate(Object left, Object right) {
        if (left instanceof Number || right instanceof Number) {
            return doCalculate(CastUtils.castNumber(left), CastUtils.castNumber(right));
        }
        if (left instanceof Date || right instanceof Date) {
            return doCalculate(CastUtils.castDate(left), CastUtils.castDate(right));
        }
        if (left instanceof String || right instanceof String) {
            return doCalculate(String.valueOf(left), String.valueOf(right));
        }
        return doCalculate(left, right);
    }

    protected Object doCalculate(Number left, Number right){
        return mapper.apply(left, right);
    }

    protected Object doCalculate(String left, String right) {
        return doCalculate(new BigDecimal(left), new BigDecimal(right));
    }

    protected Object doCalculate(Date left, Date right) {
        return doCalculate(left.getTime(), right.getTime());
    }

    protected Object doCalculate(Object left, Object right) {
        throw new UnsupportedOperationException("不支持的操作: " + left + " " + type + " " + right);
    }

}
