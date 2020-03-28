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

public class BinaryMapFeature implements ValueMapFeature {

    @Getter
    private String id;

    private BiFunction<Object, Object, Object> calculator;

    public BinaryMapFeature(String type, BiFunction<Object, Object, Object> calculator) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.calculator = calculator;
    }

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<Object, Object>, Function<Object, Object>> tuple2 = FeatureId.ValueMap.createBinaryMapper(expression, metadata);

        Function<Object, Object> leftMapper = tuple2.getT1();
        Function<Object, Object> rightMapper = tuple2.getT2();

        return v -> calculator.apply(leftMapper.apply(v), rightMapper.apply(v));
    }


}
