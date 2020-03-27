package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.util.function.Tuple2;

import java.math.BigDecimal;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BinaryCalculateMapFeature implements ValueMapFeature {

    @Getter
    private String id;
    private String type;

    private BiFunction<Number,Number,Object> calculator;

    public BinaryCalculateMapFeature(String type, BiFunction<Number,Number,Object> calculator) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.type = type;
        this.calculator = calculator;
    }

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<Object, Object>, Function<Object, Object>> tuple2 = FeatureId.ValueMap.createBinaryMapper(expression, metadata);

        Function<Object, Object> leftMapper = tuple2.getT1();
        Function<Object, Object> rightMapper = tuple2.getT2();

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
        return calculator.apply(left, right);
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
