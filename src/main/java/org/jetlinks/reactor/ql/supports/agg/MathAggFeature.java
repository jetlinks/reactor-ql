package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.stream.Collector;

public class MathAggFeature implements ValueAggMapFeature {

    private String id;

    public MathAggFeature(String type,
                          Function<Flux<Object>, Mono<?>> calculator) {
        this.id = FeatureId.ValueAggMap.of(type).getId();
        this.calculator = calculator;
    }

    private Function<Flux<Object>, Mono<?>> calculator;

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        Expression exp = function.getParameters().getExpressions().get(0);

        Function<ReactorQLRecord, ? extends Publisher<?>> fMapper = ValueMapFeature.createMapperNow(exp, metadata);

        return flux -> calculator.apply(flux.flatMap(fMapper)).cast(Object.class).flux();

    }

    @Override
    public String getId() {
        return id;
    }
}
