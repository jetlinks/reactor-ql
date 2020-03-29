package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
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
    public Function<ReactorQLContext, ? extends Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLContext, ? extends Publisher<?>>, Function<ReactorQLContext, ? extends Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLContext, ? extends Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLContext, ? extends Publisher<?>> rightMapper = tuple2.getT2();

        return v -> Mono.zip(Mono.from(leftMapper.apply(v)), Mono.from(rightMapper.apply(v)), calculator);
    }


}
