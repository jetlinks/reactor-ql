package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;

import java.util.function.Function;
import java.util.stream.Collector;

public class CollectorAggFeature implements ValueAggMapFeature {

    private String id;

    public CollectorAggFeature(String type, Function<Function<Object, ? extends Number>, Collector<Object, ?, ? extends Number>> agg) {
        this.id = FeatureId.ValueAggMap.of(type).getId();
        this.agg = agg;
    }

    private Function<Function<Object, ? extends Number>, Collector<Object, ?, ? extends Number>> agg;

    @Override
    public Function<Flux<Object>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        Expression exp = function.getParameters().getExpressions().get(0);

        Function<Object, Object> fMapper = ValueMapFeature.createMapperNow(exp, metadata);

        return flux -> flux
                .collect(agg.apply(v -> CastUtils.castNumber(fMapper.apply(v))))
                .cast(Object.class)
                .flux();

    }

    @Override
    public String getId() {
        return id;
    }
}
